package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/swiftpay-api/internal/domain"
)

// WebhookRepo gerencia persistência de webhooks.
type WebhookRepo struct {
	db *pgxpool.Pool
}

// NewWebhookRepo cria um novo repositório de webhooks.
func NewWebhookRepo(db *pgxpool.Pool) *WebhookRepo {
	return &WebhookRepo{db: db}
}

// Upsert insere ou atualiza configuração de webhook.
func (r *WebhookRepo) Upsert(ctx context.Context, wc *domain.WebhookConfig) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO webhooks (chave_pix, webhook_url, status)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (chave_pix)
		 DO UPDATE SET webhook_url = $2, status = $3, updated_at = NOW()`,
		wc.Chave, wc.WebhookURL, wc.Status,
	)
	if err != nil {
		return fmt.Errorf("upsert webhook chave=%s: %w", wc.Chave, err)
	}
	return nil
}

// GetByChave busca webhook por chave Pix.
func (r *WebhookRepo) GetByChave(ctx context.Context, chave string) (*domain.WebhookConfig, error) {
	var wc domain.WebhookConfig
	err := r.db.QueryRow(ctx,
		`SELECT chave_pix, webhook_url, status, created_at, updated_at
		 FROM webhooks WHERE chave_pix = $1`, chave,
	).Scan(&wc.Chave, &wc.WebhookURL, &wc.Status, &wc.CreatedAt, &wc.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrWebhookNaoEncontrado
		}
		return nil, fmt.Errorf("buscando webhook chave=%s: %w", chave, err)
	}
	return &wc, nil
}

// List retorna todos os webhooks configurados.
func (r *WebhookRepo) List(ctx context.Context) ([]domain.WebhookConfig, error) {
	rows, err := r.db.Query(ctx,
		`SELECT chave_pix, webhook_url, status, created_at, updated_at
		 FROM webhooks ORDER BY chave_pix`,
	)
	if err != nil {
		return nil, fmt.Errorf("listando webhooks: %w", err)
	}
	defer rows.Close()

	var webhooks []domain.WebhookConfig
	for rows.Next() {
		var wc domain.WebhookConfig
		if err := rows.Scan(
			&wc.Chave, &wc.WebhookURL, &wc.Status,
			&wc.CreatedAt, &wc.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scaneando webhook: %w", err)
		}
		webhooks = append(webhooks, wc)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterando webhooks: %w", err)
	}

	return webhooks, nil
}

// Delete remove a configuração de webhook por chave.
func (r *WebhookRepo) Delete(ctx context.Context, chave string) error {
	tag, err := r.db.Exec(ctx,
		`DELETE FROM webhooks WHERE chave_pix = $1`, chave,
	)
	if err != nil {
		return fmt.Errorf("deletando webhook chave=%s: %w", chave, err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrWebhookNaoEncontrado
	}
	return nil
}

// InsertEvent insere evento de webhook com dedup.
// Retorna true se o evento foi inserido, false se já existia (duplicado).
func (r *WebhookRepo) InsertEvent(ctx context.Context, e2eid, chave string, payload []byte) (bool, error) {
	tag, err := r.db.Exec(ctx,
		`INSERT INTO webhook_events (e2eid, chave_pix, payload)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (e2eid, chave_pix) DO NOTHING`,
		e2eid, chave, payload,
	)
	if err != nil {
		return false, fmt.Errorf("inserindo evento webhook e2eid=%s: %w", e2eid, err)
	}
	return tag.RowsAffected() == 1, nil
}

// InsertEventTx insere evento de webhook com dedup dentro da transação fornecida.
func (r *WebhookRepo) InsertEventTx(ctx context.Context, tx pgx.Tx, e2eid, chave string, payload []byte) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO webhook_events (e2eid, chave_pix, payload)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (e2eid, chave_pix) DO NOTHING`,
		e2eid, chave, payload,
	)
	if err != nil {
		return false, fmt.Errorf("inserindo evento webhook e2eid=%s: %w", e2eid, err)
	}
	return tag.RowsAffected() == 1, nil
}
