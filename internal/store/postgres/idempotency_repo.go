package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/swiftpay-api/internal/domain"
)

// IdempotencyRecord armazena o estado de uma chave de idempotência.
type IdempotencyRecord struct {
	IdempotencyKey string
	EndpointPath   string
	RequestHash    string
	Status         string
	ResponseStatus int
	ResponseBody   []byte
	StartedAt      time.Time
	CompletedAt    *time.Time
	ExpiresAt      time.Time
}

// IdempotencyRepo gerencia chaves de idempotência.
type IdempotencyRepo struct {
	db *pgxpool.Pool
}

// NewIdempotencyRepo cria um novo repositório de idempotência.
func NewIdempotencyRepo(db *pgxpool.Pool) *IdempotencyRepo {
	return &IdempotencyRepo{db: db}
}

// Acquire tenta adquirir uma chave de idempotência.
// Se a chave não existir, insere com status in_progress.
// Se existir e o hash for igual, retorna o registro existente.
// Se existir e o hash for diferente, retorna ErrIdempotencyKeyDiverged.
func (r *IdempotencyRepo) Acquire(ctx context.Context, key, endpointPath, requestHash string) (*IdempotencyRecord, error) {
	// Tenta inserir com ON CONFLICT DO NOTHING
	tag, err := r.db.Exec(ctx,
		`INSERT INTO idempotency_keys (idempotency_key, endpoint_path, request_hash, status)
		 VALUES ($1, $2, $3, 'in_progress')
		 ON CONFLICT (idempotency_key, endpoint_path) DO NOTHING`,
		key, endpointPath, requestHash,
	)
	if err != nil {
		return nil, fmt.Errorf("inserindo chave idempotencia: %w", err)
	}

	// Se inseriu (RowsAffected == 1), é nova chave
	if tag.RowsAffected() == 1 {
		return &IdempotencyRecord{
			IdempotencyKey: key,
			EndpointPath:   endpointPath,
			RequestHash:    requestHash,
			Status:         "in_progress",
			StartedAt:      time.Now(),
		}, nil
	}

	// Já existia, busca o registro
	record, err := r.Get(ctx, key, endpointPath)
	if err != nil {
		return nil, fmt.Errorf("buscando chave idempotencia existente: %w", err)
	}

	// Verifica se o hash coincide
	if record.RequestHash != requestHash {
		return record, domain.ErrIdempotencyKeyDiverged
	}

	return record, nil
}

// Get busca um registro de idempotência.
func (r *IdempotencyRepo) Get(ctx context.Context, key, endpointPath string) (*IdempotencyRecord, error) {
	var rec IdempotencyRecord
	err := r.db.QueryRow(ctx,
		`SELECT idempotency_key, endpoint_path, request_hash, status,
		 response_status, response_body, started_at, completed_at, expires_at
		 FROM idempotency_keys
		 WHERE idempotency_key = $1 AND endpoint_path = $2`,
		key, endpointPath,
	).Scan(
		&rec.IdempotencyKey, &rec.EndpointPath, &rec.RequestHash, &rec.Status,
		&rec.ResponseStatus, &rec.ResponseBody, &rec.StartedAt, &rec.CompletedAt, &rec.ExpiresAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("chave idempotencia nao encontrada: %w", err)
		}
		return nil, fmt.Errorf("buscando chave idempotencia: %w", err)
	}
	return &rec, nil
}

// Complete marca a chave como concluída com sucesso.
func (r *IdempotencyRepo) Complete(ctx context.Context, key, endpointPath string, status int, body []byte) error {
	_, err := r.db.Exec(ctx,
		`UPDATE idempotency_keys
		 SET status = 'completed', response_status = $3, response_body = $4, completed_at = NOW()
		 WHERE idempotency_key = $1 AND endpoint_path = $2`,
		key, endpointPath, status, body,
	)
	if err != nil {
		return fmt.Errorf("completando chave idempotencia: %w", err)
	}
	return nil
}

// CompleteTx marca a chave como concluída dentro de uma transação existente.
func (r *IdempotencyRepo) CompleteTx(ctx context.Context, tx pgx.Tx, key, endpointPath string, status int, body []byte) error {
	_, err := tx.Exec(ctx,
		`UPDATE idempotency_keys
		 SET status = 'completed', response_status = $3, response_body = $4, completed_at = NOW()
		 WHERE idempotency_key = $1 AND endpoint_path = $2`,
		key, endpointPath, status, body,
	)
	if err != nil {
		return fmt.Errorf("completando chave idempotencia na tx: %w", err)
	}
	return nil
}

// Fail marca a chave como falha.
func (r *IdempotencyRepo) Fail(ctx context.Context, key, endpointPath string) error {
	_, err := r.db.Exec(ctx,
		`UPDATE idempotency_keys
		 SET status = 'failed', completed_at = NOW()
		 WHERE idempotency_key = $1 AND endpoint_path = $2`,
		key, endpointPath,
	)
	if err != nil {
		return fmt.Errorf("marcando chave idempotencia como falha: %w", err)
	}
	return nil
}

// CleanupExpired remove chaves expiradas.
func (r *IdempotencyRepo) CleanupExpired(ctx context.Context) (int64, error) {
	tag, err := r.db.Exec(ctx,
		`DELETE FROM idempotency_keys WHERE expires_at < NOW()`,
	)
	if err != nil {
		return 0, fmt.Errorf("limpando chaves idempotencia expiradas: %w", err)
	}
	return tag.RowsAffected(), nil
}
