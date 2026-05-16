package mock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/matspectrum/swiftpay-api/internal/port/psp"
)

type pixRecord struct {
	E2EID             string
	TxID              string
	Valor             string
	HorarioLiquidacao time.Time
	PagadorNome       string
	PagadorCPF        string
	PagadorCNPJ       string
	InfoPagador       string
}

type devolucaoRecord struct {
	ID      string
	E2EID   string
	Valor   string
	Horario time.Time
	Status  string
}

type MockPixStore struct {
	mu         sync.RWMutex
	pixs       map[string]*pixRecord
	devolucoes map[string]*devolucaoRecord
}

func NewMockPixStore() *MockPixStore {
	return &MockPixStore{
		pixs:       make(map[string]*pixRecord),
		devolucoes: make(map[string]*devolucaoRecord),
	}
}

type PixResponse struct {
	E2EID             string `json:"e2eid"`
	TxID              string `json:"txid,omitempty"`
	Valor             string `json:"valor"`
	HorarioLiquidacao string `json:"horario"`
	PagadorNome       string `json:"pagadorNome,omitempty"`
	PagadorCPF        string `json:"pagadorCpf,omitempty"`
	PagadorCNPJ       string `json:"pagadorCnpj,omitempty"`
	InfoPagador       string `json:"infoPagador,omitempty"`
}

type DevolucaoResponse struct {
	ID      string `json:"id"`
	E2EID   string `json:"e2eid"`
	Valor   string `json:"valor"`
	Horario string `json:"horario"`
	Status  string `json:"status"`
}

func (m *MockPSP) GetPix(ctx context.Context, e2eid string) (*psp.PixResponse, error) {
	m.pixs.mu.RLock()
	defer m.pixs.mu.RUnlock()

	pix, ok := m.pixs.pixs[e2eid]
	if !ok {
		return nil, fmt.Errorf("pix nao encontrado: %s", e2eid)
	}

	return &psp.PixResponse{
		E2EID:             pix.E2EID,
		TxID:              pix.TxID,
		Valor:             pix.Valor,
		HorarioLiquidacao: pix.HorarioLiquidacao.Format(time.RFC3339),
		PagadorNome:       pix.PagadorNome,
		PagadorCPF:        pix.PagadorCPF,
		PagadorCNPJ:       pix.PagadorCNPJ,
		InfoPagador:       pix.InfoPagador,
	}, nil
}

func (m *MockPSP) ListPix(ctx context.Context, inicio, fim string, limit, offset int) ([]psp.PixResponse, int, error) {
	m.pixs.mu.RLock()
	defer m.pixs.mu.RUnlock()

	var result []psp.PixResponse
	for _, pix := range m.pixs.pixs {
		result = append(result, psp.PixResponse{
			E2EID:             pix.E2EID,
			TxID:              pix.TxID,
			Valor:             pix.Valor,
			HorarioLiquidacao: pix.HorarioLiquidacao.Format(time.RFC3339),
			PagadorNome:       pix.PagadorNome,
			PagadorCPF:        pix.PagadorCPF,
			PagadorCNPJ:       pix.PagadorCNPJ,
			InfoPagador:       pix.InfoPagador,
		})
	}

	total := len(result)
	if offset >= len(result) {
		return []psp.PixResponse{}, total, nil
	}
	result = result[offset:]
	if limit > 0 && limit < len(result) {
		result = result[:limit]
	}

	return result, total, nil
}

func (m *MockPSP) CreateDevolucao(ctx context.Context, e2eid, id, valor string) (*psp.DevolucaoResponse, error) {
	m.pixs.mu.Lock()
	defer m.pixs.mu.Unlock()

	if _, ok := m.pixs.pixs[e2eid]; !ok {
		return nil, fmt.Errorf("pix nao encontrado: %s", e2eid)
	}

	if id == "" {
		id = uuid.New().String()
	}

	now := time.Now()
	dev := &devolucaoRecord{
		ID:      id,
		E2EID:   e2eid,
		Valor:   valor,
		Horario: now,
		Status:  "EM_PROCESSAMENTO",
	}
	m.pixs.devolucoes[id] = dev

	return &psp.DevolucaoResponse{
		ID:      dev.ID,
		E2EID:   dev.E2EID,
		Valor:   dev.Valor,
		Horario: now.Format(time.RFC3339),
		Status:  dev.Status,
	}, nil
}

func (s *MockPixStore) AddPix(e2eid, txid, valor string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pixs[e2eid] = &pixRecord{
		E2EID:             e2eid,
		TxID:              txid,
		Valor:             valor,
		HorarioLiquidacao: time.Now(),
		PagadorNome:       "João Silva",
		PagadorCPF:        "01234567890",
	}
}
