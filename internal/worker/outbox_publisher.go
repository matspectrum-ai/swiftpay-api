package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/observability"
	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
)

type OutboxPublisher struct {
	db             *pgxpool.Pool
	reader         *postgres.OutboxReader
	handlers       map[string]OutboxHandler
	pollInterval   time.Duration
	retryConfig    RetryConfig
	wg             sync.WaitGroup
}

type OutboxHandler func(ctx context.Context, msg postgres.OutboxMessage) error

func NewOutboxPublisher(db *pgxpool.Pool, reader *postgres.OutboxReader, pollInterval time.Duration) *OutboxPublisher {
	return &OutboxPublisher{
		db:           db,
		reader:       reader,
		handlers:     make(map[string]OutboxHandler),
		pollInterval: pollInterval,
		retryConfig:  DefaultRetryConfig(),
	}
}

func (p *OutboxPublisher) RegisterHandler(eventType string, handler OutboxHandler) {
	p.handlers[eventType] = handler
}

func (p *OutboxPublisher) Start(ctx context.Context) error {
	slog.InfoContext(ctx, "outbox publisher iniciado",
		"poll_interval", p.pollInterval.String(),
	)

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "outbox publisher parando — aguardando lote em voo")
			p.wg.Wait()
			slog.InfoContext(ctx, "outbox publisher parado com sucesso")
			return ctx.Err()
		case <-ticker.C:
			p.updateOutboxLag(ctx)
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				if err := p.processBatch(ctx); err != nil {
					observability.WorkerErrors.WithLabelValues("outbox_publisher").Inc()
					slog.ErrorContext(ctx, "erro processando batch outbox", "error", err)
				}
			}()
		}
	}
}

func (p *OutboxPublisher) updateOutboxLag(ctx context.Context) {
	var oldestCreated *time.Time
	err := p.db.QueryRow(ctx,
		`SELECT created_at FROM outbox_messages
		 WHERE published_at IS NULL AND attempts < max_attempts
		 ORDER BY created_at ASC LIMIT 1`,
	).Scan(&oldestCreated)
	if err != nil || oldestCreated == nil {
		observability.OutboxLag.Set(0)
		return
	}
	observability.OutboxLag.Set(time.Since(*oldestCreated).Seconds())
}

func (p *OutboxPublisher) processBatch(ctx context.Context) error {
	messages, err := p.reader.ClaimAndFetch(ctx, 50, postgres.GenerateWorkerID())
	if err != nil {
		return fmt.Errorf("claim and fetch: %w", err)
	}

	if len(messages) == 0 {
		return nil
	}

	slog.DebugContext(ctx, "processando lote outbox", "count", len(messages))

	for _, msg := range messages {
		handler, ok := p.handlers[msg.EventType]
		if !ok {
			slog.WarnContext(ctx, "handler não registrado para evento",
				"event_type", msg.EventType,
				"aggregate_id", msg.AggregateID,
			)
			if err := p.reader.AckPublished(ctx, msg.ID, msg.ClaimedBy); err != nil {
				slog.ErrorContext(ctx, "erro marcando publicado", "id", msg.ID, "error", err)
			}
			continue
		}

		handlerErr := handler(ctx, msg)

		if handlerErr != nil {
			observability.RetryCount.WithLabelValues("outbox", "failure").Inc()
			slog.ErrorContext(ctx, "erro processando mensagem outbox",
				"id", msg.ID,
				"event_type", msg.EventType,
				"error", handlerErr,
			)

			if msg.Attempts+1 >= msg.MaxAttempts {
				observability.WorkerErrors.WithLabelValues("outbox_deadletter").Inc()
				if moveErr := p.reader.MoveToDeadLetter(ctx, msg, msg.ClaimedBy); moveErr != nil {
					slog.ErrorContext(ctx, "erro movendo para deadletter", "id", msg.ID, "error", moveErr)
				}
			} else {
				observability.WorkerErrors.WithLabelValues("outbox_retry").Inc()
				if nackErr := p.reader.NackFailed(ctx, msg.ID, msg.ClaimedBy, handlerErr.Error()); nackErr != nil {
					slog.ErrorContext(ctx, "erro marcando falha", "id", msg.ID, "error", nackErr)
				}
			}
			continue
		}

		if err := p.reader.AckPublished(ctx, msg.ID, msg.ClaimedBy); err != nil {
			slog.ErrorContext(ctx, "erro marcando publicado", "id", msg.ID, "error", err)
		}
	}

	return nil
}

func CobrancaCriadaHandler(ctx context.Context, msg postgres.OutboxMessage) error {
	var cob domain.Cobranca
	if err := json.Unmarshal(msg.Payload, &cob); err != nil {
		return fmt.Errorf("deserializando cobrança: %w", err)
	}
	slog.InfoContext(ctx, "evento: cobrança criada",
		"txid", cob.TxID,
		"status", cob.Status,
	)
	return nil
}

func CobrancaAtualizadaHandler(ctx context.Context, msg postgres.OutboxMessage) error {
	var cob domain.Cobranca
	if err := json.Unmarshal(msg.Payload, &cob); err != nil {
		return fmt.Errorf("deserializando cobrança: %w", err)
	}
	slog.InfoContext(ctx, "evento: cobrança atualizada",
		"txid", cob.TxID,
		"status", cob.Status,
	)
	return nil
}

func PixRecebidoHandler(ctx context.Context, msg postgres.OutboxMessage) error {
	var pix domain.PixRecebido
	if err := json.Unmarshal(msg.Payload, &pix); err != nil {
		return fmt.Errorf("deserializando pix: %w", err)
	}
	slog.InfoContext(ctx, "evento: pix recebido",
		"e2eid", pix.EndToEndID,
		"valor", int64(pix.ValorCentavos),
	)
	return nil
}

func DevolucaoSolicitadaHandler(ctx context.Context, msg postgres.OutboxMessage) error {
	var dev domain.Devolucao
	if err := json.Unmarshal(msg.Payload, &dev); err != nil {
		return fmt.Errorf("deserializando devolução: %w", err)
	}
	slog.InfoContext(ctx, "evento: devolução solicitada (idempotencia via outbox msg_id)",
		"id", dev.ID,
		"e2eid", dev.EndToEndID,
		"valor", dev.Valor,
		"outbox_msg_id", msg.ID,
	)
	return nil
}

func WebhookConfiguradoHandler(ctx context.Context, msg postgres.OutboxMessage) error {
	var wc domain.WebhookConfig
	if err := json.Unmarshal(msg.Payload, &wc); err != nil {
		return fmt.Errorf("deserializando webhook: %w", err)
	}
	slog.InfoContext(ctx, "evento: webhook configurado (pendente registro PSP)",
		"chave", wc.Chave,
	)
	return nil
}
