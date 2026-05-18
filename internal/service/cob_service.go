package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/observability"
	"github.com/matspectrum/swiftpay-api/internal/port/http/middleware"
	"github.com/matspectrum/swiftpay-api/internal/port/psp"
	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
)

type CobService struct {
	db              *pgxpool.Pool
	cobRepo         *postgres.CobRepo
	pspClient       psp.PSPClient
	outboxWriter    *postgres.OutboxWriter
	idempotencyRepo *postgres.IdempotencyRepo
	ledgerRepo      *postgres.LedgerRepo
}

func NewCobService(db *pgxpool.Pool, cobRepo *postgres.CobRepo, pspClient psp.PSPClient, outboxWriter *postgres.OutboxWriter, idempotencyRepo *postgres.IdempotencyRepo, ledgerRepo *postgres.LedgerRepo) *CobService {
	return &CobService{
		db:              db,
		cobRepo:         cobRepo,
		pspClient:       pspClient,
		outboxWriter:    outboxWriter,
		idempotencyRepo: idempotencyRepo,
		ledgerRepo:      ledgerRepo,
	}
}

func (s *CobService) CreateCob(ctx context.Context, cob *domain.Cobranca) (*domain.Cobranca, bool, error) {
	cob.Sanitize()

	if err := cob.Validate(); err != nil {
		return nil, false, err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("iniciando transação: %w", err)
	}
	defer tx.Rollback(ctx)

	existing, err := s.cobRepo.GetByTxID(ctx, cob.TxID)
	if err == nil && existing != nil {
		tx.Rollback(ctx)
		if existing.Chave == cob.Chave && existing.Valor.Original == cob.Valor.Original {
			return existing, false, nil
		}
		return nil, false, domain.FormatValidationError("txid %s já existe com dados diferentes", cob.TxID)
	}

	cob.Status = domain.CobStatusAtiva
	if err := s.cobRepo.Create(ctx, tx, cob); err != nil {
		return nil, false, fmt.Errorf("salvando cobrança: %w", err)
	}

	if err := s.outboxWriter.Write(ctx, tx, "cobranca", cob.TxID, "CobrancaCriada", cob); err != nil {
		return nil, false, fmt.Errorf("escrevendo outbox: %w", err)
	}

	key := domain.IdempotencyKeyFromContext(ctx)
	if key != "" {
		responseBody, err := json.Marshal(cob)
		if err != nil {
			return nil, false, fmt.Errorf("serializando cobrança: %w", err)
		}
		if err := s.idempotencyRepo.CompleteTx(ctx, tx, key, domain.EndpointPathFromContext(ctx), http.StatusCreated, responseBody); err != nil {
			return nil, false, fmt.Errorf("completando idempotencia: %w", err)
		}
	}

	nextStateJSON, err := json.Marshal(cob)
	if err != nil {
		return nil, false, fmt.Errorf("serializando cobrança: %w", err)
	}
	if s.ledgerRepo != nil {
		if err := s.ledgerRepo.Append(ctx, tx, &postgres.LedgerEvent{
			EventType:       "cobranca_criada",
			AggregateType:   "cobranca",
			AggregateID:     cob.TxID,
			CorrelationID:   middleware.GetRequestID(ctx),
			RequestID:       middleware.GetRequestID(ctx),
			TxID:            cob.TxID,
			NextState:       nextStateJSON,
			OperationSource: "api",
		}); err != nil {
			slog.WarnContext(ctx, "erro escrevendo evento ledger", "error", err, "txid", cob.TxID)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit transação: %w", err)
	}

	cobReq := psp.CobRequest{
		Calendar:   cob.Calendar,
		Devedor:    cob.Devedor,
		Valor:      cob.Valor,
		Chave:      cob.Chave,
		SolPagador: cob.SolicitacaoPagador,
	}

	pspStart := time.Now().UTC()
	pspResp, err := s.pspClient.CreateCob(ctx, cob.TxID, cobReq)
	observability.PSPLatency.WithLabelValues("create_cob").Observe(time.Since(pspStart).Seconds())

	if err != nil {
		slog.ErrorContext(ctx, "psp criar cobrança falhou após commit local", "error", err, "txid", cob.TxID)
		return cob, true, nil
	}

	cob.Revisao = pspResp.Revisao
	cob.Location = pspResp.Location
	cob.PixCopiaECola = pspResp.PixCopiaECola
	cob.Calendar = pspResp.Calendar

	updateTx, err := s.db.Begin(ctx)
	if err != nil {
		slog.WarnContext(ctx, "falha ao iniciar tx de update psp", "error", err)
		return cob, true, nil
	}
	defer updateTx.Rollback(ctx)

	if err := s.cobRepo.Update(ctx, updateTx, cob, 0); err != nil {
		slog.WarnContext(ctx, "falha ao atualizar dados PSP", "error", err)
		return cob, true, nil
	}

	if err := updateTx.Commit(ctx); err != nil {
		slog.WarnContext(ctx, "falha ao commitar update PSP", "error", err)
		return cob, true, nil
	}

	slog.InfoContext(ctx, "cobrança criada", "txid", cob.TxID, "status", cob.Status)
	return cob, true, nil
}

func (s *CobService) UpdateCob(ctx context.Context, txid string, cob *domain.Cobranca) (*domain.Cobranca, error) {
	cob.Sanitize()
	cob.TxID = txid

	if err := cob.Validate(); err != nil {
		return nil, err
	}

	existing, err := s.cobRepo.GetByTxID(ctx, txid)
	if err != nil {
		return nil, err
	}

	if existing.Status != domain.CobStatusAtiva {
		return nil, domain.FormatValidationError("cobrança com status %s não pode ser alterada", existing.Status)
	}

	cobReq := psp.CobRequest{
		Calendar:   cob.Calendar,
		Devedor:    cob.Devedor,
		Valor:      cob.Valor,
		Chave:      cob.Chave,
		SolPagador: cob.SolicitacaoPagador,
	}

	pspStart := time.Now().UTC()
	pspResp, err := s.pspClient.UpdateCob(ctx, txid, cobReq)
	observability.PSPLatency.WithLabelValues("update_cob").Observe(time.Since(pspStart).Seconds())

	if err != nil {
		return nil, fmt.Errorf("psp atualizar cobrança: %w", err)
	}

	cob.Revisao = pspResp.Revisao
	cob.Status = existing.Status
	cob.Location = pspResp.Location
	cob.PixCopiaECola = pspResp.PixCopiaECola

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("iniciando transação: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := s.cobRepo.Update(ctx, tx, cob, existing.Revisao); err != nil {
		return nil, fmt.Errorf("atualizando cobrança: %w", err)
	}

	if err := s.outboxWriter.Write(ctx, tx, "cobranca", cob.TxID, "CobrancaAtualizada", cob); err != nil {
		return nil, fmt.Errorf("escrevendo outbox: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transação: %w", err)
	}

	slog.InfoContext(ctx, "cobrança atualizada", "txid", cob.TxID)
	return cob, nil
}

func (s *CobService) PatchCob(ctx context.Context, txid string, patch *domain.CobrancaPatch) (*domain.Cobranca, error) {
	if err := patch.Validate(); err != nil {
		return nil, err
	}

	existing, err := s.cobRepo.GetByTxID(ctx, txid)
	if err != nil {
		return nil, err
	}

	if !existing.Status.CanTransitionTo(patch.Status) {
		return nil, domain.FormatValidationError(
			"transição de %s para %s não é permitida",
			existing.Status, patch.Status,
		)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("iniciando transação: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := s.cobRepo.UpdateStatus(ctx, tx, txid, patch.Status, existing.Revisao); err != nil {
		return nil, fmt.Errorf("atualizando status: %w", err)
	}

	existing.Status = patch.Status

	if err := s.outboxWriter.Write(ctx, tx, "cobranca", txid, "CobrancaAtualizada", existing); err != nil {
		return nil, fmt.Errorf("escrevendo outbox: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transação: %w", err)
	}

	slog.InfoContext(ctx, "status cobrança atualizado", "txid", txid, "status", patch.Status)
	return existing, nil
}

func (s *CobService) GetCob(ctx context.Context, txid string) (*domain.Cobranca, error) {
	return s.cobRepo.GetByTxID(ctx, txid)
}

func (s *CobService) ListCobs(ctx context.Context, filter domain.CobFilter) ([]domain.Cobranca, int, error) {
	return s.cobRepo.List(ctx, filter)
}
