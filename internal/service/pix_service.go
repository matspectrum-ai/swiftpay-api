package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/port/psp"
	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
)

type PixService struct {
	db           *pgxpool.Pool
	pixRepo      *postgres.PixRepo
	cobRepo      *postgres.CobRepo
	pspClient    psp.PSPClient
	outboxWriter *postgres.OutboxWriter
}

func NewPixService(db *pgxpool.Pool, pixRepo *postgres.PixRepo, cobRepo *postgres.CobRepo, pspClient psp.PSPClient, outboxWriter *postgres.OutboxWriter) *PixService {
	return &PixService{
		db:           db,
		pixRepo:      pixRepo,
		cobRepo:      cobRepo,
		pspClient:    pspClient,
		outboxWriter: outboxWriter,
	}
}

func (s *PixService) ProcessPixRecebido(ctx context.Context, pix *domain.PixRecebido) error {
	existing, err := s.pixRepo.GetByE2EID(ctx, pix.E2EID)
	if err == nil && existing != nil {
		slog.InfoContext(ctx, "pix já processado (dedup)", "e2eid", pix.E2EID)
		return nil
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("iniciando transação pix: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := s.pixRepo.Create(ctx, tx, pix); err != nil {
		return fmt.Errorf("salvando pix: %w", err)
	}

	if pix.TxID != "" {
		cob, err := s.cobRepo.GetByTxID(ctx, pix.TxID)
		if err != nil {
			slog.WarnContext(ctx, "cobrança não encontrada para pix", "txid", pix.TxID)
		} else if !cob.Status.CanTransitionTo(domain.CobStatusConcluida) {
			slog.WarnContext(ctx, "transição FSM inválida ignorada",
				"txid", pix.TxID,
				"status_atual", cob.Status,
				"status_desejado", domain.CobStatusConcluida,
			)
		} else if pix.ValorCentavos < cob.Valor.Original {
			slog.WarnContext(ctx, "pix com valor inferior à cobrança — conclusão recusada",
				"txid", pix.TxID,
				"valor_pix", int64(pix.ValorCentavos),
				"valor_cobranca", int64(cob.Valor.Original),
			)
		} else {
			if err := s.cobRepo.UpdateStatus(ctx, tx, pix.TxID, domain.CobStatusConcluida, cob.Revisao); err != nil {
				return fmt.Errorf("atualizando status cobrança: %w", err)
			}
		}
	}

	if err := s.outboxWriter.Write(ctx, tx, "pix", pix.E2EID, "PixRecebido", pix); err != nil {
		return fmt.Errorf("escrevendo outbox pix: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transação pix: %w", err)
	}

	slog.InfoContext(ctx, "pix recebido processado", "e2eid", pix.E2EID, "valor", int64(pix.ValorCentavos))
	return nil
}

func (s *PixService) GetPix(ctx context.Context, e2eid string) (*domain.PixRecebido, error) {
	return s.pixRepo.GetByE2EID(ctx, e2eid)
}

func (s *PixService) ListPix(ctx context.Context, filter domain.PixFilter) ([]domain.PixRecebido, int, error) {
	return s.pixRepo.List(ctx, filter)
}

func (s *PixService) CreateDevolucao(ctx context.Context, e2eid, devID, valorStr string) (*domain.Devolucao, error) {
	existing, err := s.pixRepo.GetByE2EID(ctx, e2eid)
	if err != nil {
		return nil, fmt.Errorf("pix nao encontrado para devolucao: %w", err)
	}

	var valor domain.ValorCentavos
	if err := valor.UnmarshalJSON([]byte(`"` + valorStr + `"`)); err != nil {
		return nil, fmt.Errorf("valor inválido para devolução: %w", err)
	}

	dev := &domain.Devolucao{
		ID:      devID,
		E2EID:   e2eid,
		Valor:   valor,
		Status:  "PENDENTE",
		Horario: existing.HorarioLiquidacao,
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("iniciando transação devolucao: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := s.pixRepo.CreateDevolucao(ctx, tx, dev); err != nil {
		return nil, fmt.Errorf("salvando devolucao: %w", err)
	}

	if err := s.outboxWriter.Write(ctx, tx, "pix", e2eid, "DevolucaoSolicitada", dev); err != nil {
		return nil, fmt.Errorf("escrevendo outbox devolucao: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transação devolucao: %w", err)
	}

	slog.InfoContext(ctx, "devolução solicitada (pendente PSP)", "e2eid", e2eid, "devolucao_id", dev.ID)
	return dev, nil
}
