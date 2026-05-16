package mock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/port/psp"
)

type MockCobStore struct {
	mu   sync.RWMutex
	cobs map[string]*psp.CobResponse
}

func NewMockCobStore() *MockCobStore {
	return &MockCobStore{
		cobs: make(map[string]*psp.CobResponse),
	}
}

func (m *MockPSP) CreateCob(ctx context.Context, txid string, req psp.CobRequest) (*psp.CobResponse, error) {
	m.cobs.mu.Lock()
	defer m.cobs.mu.Unlock()

	now := time.Now()
	location := fmt.Sprintf("https://pix.example.com/api/v2/cob/%s", txid)
	pixCopiaECola := fmt.Sprintf("00020101021126360014br.gov.bcb.pix0114%s5204000053039865802BR5925Recebedor%%20Mock6009Sao%%20Paulo62070503***6304A1B2",
		txid)

	cob := &psp.CobResponse{
		TxID:     txid,
		Revisao:  0,
		Calendar: req.Calendar,
		Devedor:  req.Devedor,
		Valor:    req.Valor,
		Chave:    req.Chave,
		SolPagador:    req.SolPagador,
		Status:        domain.CobStatusAtiva,
		Location:      location,
		PixCopiaECola: pixCopiaECola,
	}

	cob.Calendar.Criacao = now
	m.cobs.cobs[txid] = cob

	resp := *cob
	return &resp, nil
}

func (m *MockPSP) UpdateCob(ctx context.Context, txid string, req psp.CobRequest) (*psp.CobResponse, error) {
	m.cobs.mu.Lock()
	defer m.cobs.mu.Unlock()

	existing, ok := m.cobs.cobs[txid]
	if !ok {
		return nil, fmt.Errorf("cobranca nao encontrada: %s", txid)
	}

	existing.Calendar = req.Calendar
	existing.Devedor = req.Devedor
	existing.Valor = req.Valor
	existing.Chave = req.Chave
	existing.SolPagador = req.SolPagador
	existing.Revisao++

	resp := *existing
	return &resp, nil
}

func (m *MockPSP) GetCob(ctx context.Context, txid string) (*psp.CobResponse, error) {
	m.cobs.mu.RLock()
	defer m.cobs.mu.RUnlock()

	cob, ok := m.cobs.cobs[txid]
	if !ok {
		return nil, fmt.Errorf("cobranca nao encontrada: %s", txid)
	}

	resp := *cob
	return &resp, nil
}

func (m *MockPSP) ListCobs(ctx context.Context, inicio, fim string, limit, offset int) ([]psp.CobResponse, int, error) {
	m.cobs.mu.RLock()
	defer m.cobs.mu.RUnlock()

	var result []psp.CobResponse
	for _, cob := range m.cobs.cobs {
		result = append(result, *cob)
	}

	total := len(result)

	if offset >= len(result) {
		return []psp.CobResponse{}, total, nil
	}
	result = result[offset:]

	if limit > 0 && limit < len(result) {
		result = result[:limit]
	}

	return result, total, nil
}
