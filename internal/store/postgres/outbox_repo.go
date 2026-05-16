package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OutboxMessage representa uma mensagem no outbox transacional.
type OutboxMessage struct {
	ID            string
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       json.RawMessage
	CreatedAt     time.Time
	PublishedAt   *time.Time
	Attempts      int
	MaxAttempts   int
	LastError     string
	ClaimedAt     *time.Time
	ClaimedBy     string
	LeaseTimeout  int
}

// OutboxWriter escreve mensagens no outbox dentro de uma transação.
type OutboxWriter struct {
	db *pgxpool.Pool
}

// NewOutboxWriter cria um novo OutboxWriter.
func NewOutboxWriter(db *pgxpool.Pool) *OutboxWriter {
	return &OutboxWriter{db: db}
}

// Write insere uma mensagem no outbox dentro da transação fornecida.
func (w *OutboxWriter) Write(ctx context.Context, tx pgx.Tx, aggregateType, aggregateID, eventType string, payload interface{}) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("serializando payload outbox: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox_messages (aggregate_type, aggregate_id, event_type, payload)
		 VALUES ($1, $2, $3, $4)`,
		aggregateType, aggregateID, eventType, payloadJSON,
	)
	if err != nil {
		return fmt.Errorf("escrevendo mensagem outbox: %w", err)
	}
	return nil
}

// OutboxReader lê mensagens pendentes do outbox.
type OutboxReader struct {
	db *pgxpool.Pool
}

// NewOutboxReader cria um novo OutboxReader.
func NewOutboxReader(db *pgxpool.Pool) *OutboxReader {
	return &OutboxReader{db: db}
}

// FetchPending busca mensagens não publicadas com lock pessimista.
func (r *OutboxReader) FetchPending(ctx context.Context, limit int) ([]OutboxMessage, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
		 FROM outbox_messages
		 WHERE published_at IS NULL AND attempts < max_attempts
		 ORDER BY created_at ASC
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("buscando mensagens outbox pendentes: %w", err)
	}
	defer rows.Close()

	var messages []OutboxMessage
	for rows.Next() {
		var msg OutboxMessage
		if err := rows.Scan(
			&msg.ID, &msg.AggregateType, &msg.AggregateID,
			&msg.EventType, &msg.Payload, &msg.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scaneando mensagem outbox: %w", err)
		}
		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterando mensagens outbox: %w", err)
	}

	return messages, nil
}

// FetchPendingTx busca mensagens não publicadas com lock pessimista dentro da transação fornecida.
func (r *OutboxReader) FetchPendingTx(ctx context.Context, tx pgx.Tx, limit int) ([]OutboxMessage, error) {
	rows, err := tx.Query(ctx,
		`SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
		 FROM outbox_messages
		 WHERE published_at IS NULL AND attempts < max_attempts
		 ORDER BY created_at ASC
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("buscando mensagens outbox pendentes: %w", err)
	}
	defer rows.Close()

	var messages []OutboxMessage
	for rows.Next() {
		var msg OutboxMessage
		if err := rows.Scan(
			&msg.ID, &msg.AggregateType, &msg.AggregateID,
			&msg.EventType, &msg.Payload, &msg.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scaneando mensagem outbox: %w", err)
		}
		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterando mensagens outbox: %w", err)
	}

	return messages, nil
}

// MarkPublished marca uma mensagem como publicada.
func (r *OutboxReader) MarkPublished(ctx context.Context, id string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE outbox_messages SET published_at = NOW(), attempts = attempts + 1 WHERE id = $1`,
		id,
	)
	if err != nil {
		return fmt.Errorf("marcando outbox publicado id=%s: %w", id, err)
	}
	return nil
}

// MarkPublishedTx marca uma mensagem como publicada dentro da transação fornecida.
func (r *OutboxReader) MarkPublishedTx(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx,
		`UPDATE outbox_messages SET published_at = NOW(), attempts = attempts + 1 WHERE id = $1`,
		id,
	)
	if err != nil {
		return fmt.Errorf("marcando outbox publicado id=%s: %w", id, err)
	}
	return nil
}

// MarkFailed marca uma mensagem como falha e incrementa tentativas.
func (r *OutboxReader) MarkFailed(ctx context.Context, id string, lastError string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE outbox_messages
		 SET attempts = attempts + 1, last_error = $2
		 WHERE id = $1`,
		id, lastError,
	)
	if err != nil {
		return fmt.Errorf("marcando outbox falho id=%s: %w", id, err)
	}
	return nil
}

// MarkFailedTx marca uma mensagem como falha dentro da transação fornecida.
func (r *OutboxReader) MarkFailedTx(ctx context.Context, tx pgx.Tx, id string, lastError string) error {
	_, err := tx.Exec(ctx,
		`UPDATE outbox_messages
		 SET attempts = attempts + 1, last_error = $2
		 WHERE id = $1`,
		id, lastError,
	)
	if err != nil {
		return fmt.Errorf("marcando outbox falho id=%s: %w", id, err)
	}
	return nil
}

// ClaimAndFetch inicia transação, dá lock nas mensagens pendentes e marca claimed_at/claimed_by.
func (r *OutboxReader) ClaimAndFetch(ctx context.Context, limit int, workerID string) ([]OutboxMessage, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx claim: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx,
		`UPDATE outbox_messages SET claimed_at = NOW(), claimed_by = $1
		 WHERE id IN (
			 SELECT id FROM outbox_messages
			 WHERE published_at IS NULL AND attempts < max_attempts
			   AND (claimed_at IS NULL OR claimed_at + (lease_timeout || ' seconds')::INTERVAL < NOW())
			 ORDER BY created_at ASC LIMIT $2 FOR UPDATE SKIP LOCKED
		 )
		 RETURNING id, aggregate_type, aggregate_id, event_type, payload, created_at, attempts, max_attempts, last_error`,
		workerID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("claiming messages: %w", err)
	}
	defer rows.Close()

	var messages []OutboxMessage
	for rows.Next() {
		var msg OutboxMessage
		if err := rows.Scan(
			&msg.ID, &msg.AggregateType, &msg.AggregateID,
			&msg.EventType, &msg.Payload, &msg.CreatedAt,
			&msg.Attempts, &msg.MaxAttempts, &msg.LastError,
		); err != nil {
			return nil, fmt.Errorf("scanning claimed message: %w", err)
		}
		messages = append(messages, msg)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}

	return messages, nil
}

// AckPublished marca mensagem como publicada e libera claim.
func (r *OutboxReader) AckPublished(ctx context.Context, id, workerID string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE outbox_messages
		 SET published_at = NOW(), attempts = attempts + 1, claimed_at = NULL, claimed_by = NULL
		 WHERE id = $1 AND claimed_by = $2`,
		id, workerID,
	)
	if err != nil {
		return fmt.Errorf("ack published id=%s: %w", id, err)
	}
	return nil
}

// NackFailed incrementa tentativas e libera claim após falha.
func (r *OutboxReader) NackFailed(ctx context.Context, id, workerID, lastErr string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE outbox_messages
		 SET attempts = attempts + 1, last_error = $3, claimed_at = NULL, claimed_by = NULL
		 WHERE id = $1 AND claimed_by = $2`,
		id, workerID, lastErr,
	)
	if err != nil {
		return fmt.Errorf("nack failed id=%s: %w", id, err)
	}
	return nil
}

// MoveToDeadLetter move mensagem para deadletter e marca original como publicada.
func (r *OutboxReader) MoveToDeadLetter(ctx context.Context, msg OutboxMessage) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx deadletter: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox_dead_letter (original_id, aggregate_type, aggregate_id, event_type, payload, attempts, last_error, moved_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())`,
		msg.ID, msg.AggregateType, msg.AggregateID, msg.EventType, msg.Payload, msg.Attempts, msg.LastError,
	)
	if err != nil {
		return fmt.Errorf("moving to deadletter: %w", err)
	}

	_, err = tx.Exec(ctx, `UPDATE outbox_messages SET published_at = NOW() WHERE id = $1`, msg.ID)
	if err != nil {
		return fmt.Errorf("marking deadletter complete: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit deadletter: %w", err)
	}

	return nil
}

// GenerateWorkerID gera um identificador único de worker.
func GenerateWorkerID() string {
	return uuid.New().String()
}
