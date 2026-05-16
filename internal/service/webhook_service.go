package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/observability"
	"github.com/matspectrum/swiftpay-api/internal/port/http/middleware"
	"github.com/matspectrum/swiftpay-api/internal/port/psp"
	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
)

type WebhookService struct {
	db           *pgxpool.Pool
	webhookRepo  *postgres.WebhookRepo
	pixRepo      *postgres.PixRepo
	cobRepo      *postgres.CobRepo
	pixService   *PixService
	pspClient    psp.PSPClient
	outboxWriter *postgres.OutboxWriter
	ledgerRepo   *postgres.LedgerRepo
}

func NewWebhookService(db *pgxpool.Pool, webhookRepo *postgres.WebhookRepo, pixRepo *postgres.PixRepo, cobRepo *postgres.CobRepo, pixService *PixService, pspClient psp.PSPClient, outboxWriter *postgres.OutboxWriter, ledgerRepo *postgres.LedgerRepo) *WebhookService {
	return &WebhookService{
		db:           db,
		webhookRepo:  webhookRepo,
		pixRepo:      pixRepo,
		cobRepo:      cobRepo,
		pixService:   pixService,
		pspClient:    pspClient,
		outboxWriter: outboxWriter,
		ledgerRepo:   ledgerRepo,
	}
}

func (s *WebhookService) ConfigureWebhook(ctx context.Context, chave, webhookURL string) (*domain.WebhookConfig, error) {
	if chave == "" {
		return nil, domain.FormatValidationError("chave é obrigatória")
	}

	if _, err := url.ParseRequestURI(webhookURL); err != nil {
		return nil, domain.FormatValidationError("url de webhook inválida: %s", webhookURL)
	}

	wc := &domain.WebhookConfig{
		Chave:      chave,
		WebhookURL: webhookURL,
		Status:     "PENDENTE",
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("iniciando transação webhook: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := s.webhookRepo.Upsert(ctx, wc); err != nil {
		return nil, fmt.Errorf("salvando webhook local: %w", err)
	}

	if err := s.outboxWriter.Write(ctx, tx, "webhook", chave, "WebhookConfigurado", wc); err != nil {
		return nil, fmt.Errorf("escrevendo outbox webhook: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transação webhook: %w", err)
	}

	slog.InfoContext(ctx, "webhook configurado (pendente PSP)", "chave", chave)
	return wc, nil
}

func (s *WebhookService) GetWebhook(ctx context.Context, chave string) (*domain.WebhookConfig, error) {
	return s.webhookRepo.GetByChave(ctx, chave)
}

func (s *WebhookService) ListWebhooks(ctx context.Context) ([]domain.WebhookConfig, error) {
	return s.webhookRepo.List(ctx)
}

func (s *WebhookService) DeleteWebhook(ctx context.Context, chave string) error {
	if err := s.pspClient.DeleteWebhook(ctx, chave); err != nil {
		return fmt.Errorf("psp deletar webhook: %w", err)
	}

	if err := s.webhookRepo.Delete(ctx, chave); err != nil {
		return fmt.Errorf("deletando webhook local: %w", err)
	}

	slog.InfoContext(ctx, "webhook removido", "chave", chave)
	return nil
}

func (s *WebhookService) HandleCallback(ctx context.Context, payload []byte) error {
	var wp domain.WebhookPayload
	if err := json.Unmarshal(payload, &wp); err != nil {
		return fmt.Errorf("decodificando payload webhook: %w", err)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("iniciando transação: %w", err)
	}
	defer tx.Rollback(ctx)

	inserted, err := s.webhookRepo.InsertEventTx(ctx, tx, wp.E2EID, wp.Chave, payload)
	if err != nil {
		return fmt.Errorf("inserindo evento webhook: %w", err)
	}
	if !inserted {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit transação (duplicado): %w", err)
		}
		slog.InfoContext(ctx, "evento webhook duplicado ignorado", "e2eid", wp.E2EID)
		observability.WebhookProcessed.WithLabelValues("duplicate").Inc()
		return nil
	}

	pix := wp.ToPixRecebido()
	if err := s.pixRepo.Create(ctx, tx, pix); err != nil {
		return fmt.Errorf("persistindo pix: %w", err)
	}

	var previousCob *domain.Cobranca
	if pix.TxID != "" {
		existingCob, err := s.cobRepo.GetByTxID(ctx, pix.TxID)
		if err != nil {
			slog.WarnContext(ctx, "cobrança não encontrada para pix", "txid", pix.TxID, "error", err)
		} else if !existingCob.Status.CanTransitionTo(domain.CobStatusConcluida) {
			slog.WarnContext(ctx, "transição FSM inválida ignorada no webhook",
				"txid", pix.TxID,
				"status_atual", existingCob.Status,
				"status_desejado", domain.CobStatusConcluida,
			)
		} else {
			previousCob = existingCob
			if err := s.cobRepo.UpdateStatus(ctx, tx, pix.TxID, domain.CobStatusConcluida, existingCob.Revisao); err != nil {
				slog.WarnContext(ctx, "erro atualizando status cobrança no callback",
					"txid", pix.TxID, "error", err,
				)
			}
		}
	}

	if err := s.outboxWriter.Write(ctx, tx, "pix", pix.E2EID, "PixRecebido", pix); err != nil {
		return fmt.Errorf("escrevendo outbox: %w", err)
	}

	if s.ledgerRepo != nil && pix.TxID != "" {
		var previousStateJSON json.RawMessage
		if previousCob != nil {
			previousStateJSON, _ = json.Marshal(previousCob)
		}
		nextStateJSON, _ := json.Marshal(domain.CobStatusConcluida)
		if err := s.ledgerRepo.Append(ctx, tx, &postgres.LedgerEvent{
			EventType:       "pix_recebido",
			AggregateType:   "cobranca",
			AggregateID:     pix.TxID,
			CorrelationID:   middleware.GetRequestID(ctx),
			RequestID:       middleware.GetRequestID(ctx),
			TxID:            pix.TxID,
			E2EID:           pix.E2EID,
			PreviousState:   previousStateJSON,
			NextState:       nextStateJSON,
			OperationSource: "webhook_callback",
		}); err != nil {
			slog.WarnContext(ctx, "erro escrevendo evento ledger", "error", err, "txid", pix.TxID)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transação: %w", err)
	}

	slog.InfoContext(ctx, "pix recebido processado",
		"e2eid", pix.E2EID,
		"valor", int64(pix.ValorCentavos),
	)
	return nil
}
