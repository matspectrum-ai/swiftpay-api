package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
)

type CleanupWorker struct {
	db              *pgxpool.Pool
	outboxReader    *postgres.OutboxReader
	idempotencyRepo *postgres.IdempotencyRepo
	interval        time.Duration
	retentionDays   int
}

func NewCleanupWorker(db *pgxpool.Pool, outboxReader *postgres.OutboxReader, idempotencyRepo *postgres.IdempotencyRepo, interval time.Duration, retentionDays int) *CleanupWorker {
	return &CleanupWorker{
		db:              db,
		outboxReader:    outboxReader,
		idempotencyRepo: idempotencyRepo,
		interval:        interval,
		retentionDays:   retentionDays,
	}
}

func (w *CleanupWorker) Start(ctx context.Context) error {
	slog.InfoContext(ctx, "cleanup worker iniciado",
		"interval", w.interval.String(),
		"retention_days", w.retentionDays,
	)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "cleanup worker parando")
			return ctx.Err()
		case <-ticker.C:
			w.prune(ctx)
		}
	}
}

const (
	pruneBatchSize   = 2000
	cleanupLockID    = 8888
)

func (w *CleanupWorker) prune(ctx context.Context) {
	var acquired bool
	err := w.db.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, cleanupLockID).Scan(&acquired)
	if err != nil {
		slog.ErrorContext(ctx, "erro ao tentar adquirir lock de limpeza", "error", err)
		return
	}
	if !acquired {
		slog.InfoContext(ctx, "limpeza já em execução noutra instância — ignorando esta iteração")
		return
	}

	defer func() {
		if _, unlockErr := w.db.Exec(ctx, `SELECT pg_advisory_unlock($1)`, cleanupLockID); unlockErr != nil {
			slog.ErrorContext(ctx, "erro ao libertar lock de limpeza", "error", unlockErr)
		}
	}()

	slog.InfoContext(ctx, "lock de limpeza adquirido — iniciando limpeza em lotes")

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "limpeza interrompida por cancelamento")
			return
		default:
		}

		deleted, err := w.outboxReader.PruneOldMessagesBatch(ctx, w.retentionDays, pruneBatchSize)
		if err != nil {
			slog.ErrorContext(ctx, "erro removendo mensagens outbox antigas", "error", err)
			break
		}
		if deleted == 0 {
			break
		}
		slog.InfoContext(ctx, "lote de mensagens outbox removidas", "count", deleted)
		time.Sleep(1 * time.Second)
	}

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "limpeza interrompida por cancelamento")
			return
		default:
		}

		deleted, err := w.outboxReader.PruneOldDeadLettersBatch(ctx, w.retentionDays, pruneBatchSize)
		if err != nil {
			slog.ErrorContext(ctx, "erro removendo deadletters antigas", "error", err)
			break
		}
		if deleted == 0 {
			break
		}
		slog.InfoContext(ctx, "lote de deadletters removidas", "count", deleted)
		time.Sleep(1 * time.Second)
	}

	if deleted, err := w.idempotencyRepo.CleanupExpired(ctx); err != nil {
		slog.ErrorContext(ctx, "erro removendo chaves idempotencia expiradas", "error", err)
	} else if deleted > 0 {
		slog.InfoContext(ctx, "chaves idempotencia expiradas removidas", "count", deleted)
	}

	slog.InfoContext(ctx, "limpeza concluída — lock libertado")
}
