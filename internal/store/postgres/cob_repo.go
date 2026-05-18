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

// CobRepo gerencia persistência de cobranças.
type CobRepo struct {
	db *pgxpool.Pool
}

// NewCobRepo cria um novo repositório de cobranças.
func NewCobRepo(db *pgxpool.Pool) *CobRepo {
	return &CobRepo{db: db}
}

// Create insere uma nova cobrança.
func (r *CobRepo) Create(ctx context.Context, tx pgx.Tx, cob *domain.Cobranca) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO cobrancas (txid, chave_pix, valor_original, status,
		 calendario_criacao, calendario_expiracao, devedor_nome, devedor_cpf,
		 devedor_cnpj, solicitacao_pagador, location_url, pix_copia_e_cola, revisao)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		cob.TxID, cob.Chave, int64(cob.Valor.Original), cob.Status,
		cob.Calendar.Criacao, cob.Calendar.Criacao.Add(time.Duration(cob.Calendar.Expiracao)*time.Second),
		cob.Devedor.Nome, cob.Devedor.CPF, cob.Devedor.CNPJ,
		cob.SolicitacaoPagador, cob.Location, cob.PixCopiaECola, cob.Revisao,
	)
	if err != nil {
		return fmt.Errorf("inserindo cobrança txid=%s: %w", cob.TxID, err)
	}
	return nil
}

// Update atualiza uma cobrança existente com optimistic locking.
func (r *CobRepo) Update(ctx context.Context, tx pgx.Tx, cob *domain.Cobranca, expectedRevisao int) error {
	tag, err := tx.Exec(ctx,
		`UPDATE cobrancas SET
		 chave_pix = $2, valor_original = $3, status = $4,
		 calendario_expiracao = $5, devedor_nome = $6, devedor_cpf = $7,
		 devedor_cnpj = $8, solicitacao_pagador = $9,
		 location_url = $10, pix_copia_e_cola = $11, revisao = $12,
		 updated_at = NOW()
		 WHERE txid = $1 AND revisao = $13`,
		cob.TxID, cob.Chave, int64(cob.Valor.Original), cob.Status,
		cob.Calendar.Criacao.Add(time.Duration(cob.Calendar.Expiracao)*time.Second),
		cob.Devedor.Nome, cob.Devedor.CPF, cob.Devedor.CNPJ,
		cob.SolicitacaoPagador, cob.Location, cob.PixCopiaECola, cob.Revisao,
		expectedRevisao,
	)
	if err != nil {
		return fmt.Errorf("atualizando cobrança txid=%s: %w", cob.TxID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("conflito de versão ou cobrança não encontrada txid=%s", cob.TxID)
	}
	return nil
}

// UpdateStatus atualiza apenas o status de uma cobrança com optimistic locking.
func (r *CobRepo) UpdateStatus(ctx context.Context, tx pgx.Tx, txid string, status domain.CobStatus, expectedRevisao int) error {
	tag, err := tx.Exec(ctx,
		`UPDATE cobrancas SET status = $2, revisao = revisao + 1, updated_at = NOW() WHERE txid = $1 AND revisao = $3`,
		txid, string(status), expectedRevisao,
	)
	if err != nil {
		return fmt.Errorf("atualizando status cobrança txid=%s: %w", txid, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("conflito de versão ou cobrança não encontrada txid=%s revisao=%d", txid, expectedRevisao)
	}
	return nil
}

// GetByTxID busca cobrança por txid.
func (r *CobRepo) GetByTxID(ctx context.Context, txid string) (*domain.Cobranca, error) {
	var cob domain.Cobranca
	var statusStr string
	var calendarioExpiracao time.Time
	var valorOriginal int64

	err := r.db.QueryRow(ctx,
		`SELECT txid, chave_pix, valor_original::bigint, status,
		 calendario_criacao, calendario_expiracao, devedor_nome,
		 devedor_cpf, devedor_cnpj, solicitacao_pagador,
		 location_url, pix_copia_e_cola, revisao, created_at, updated_at
		 FROM cobrancas WHERE txid = $1`, txid,
	).Scan(
		&cob.TxID, &cob.Chave, &valorOriginal, &statusStr,
		&cob.Calendar.Criacao, &calendarioExpiracao, &cob.Devedor.Nome,
		&cob.Devedor.CPF, &cob.Devedor.CNPJ, &cob.SolicitacaoPagador,
		&cob.Location, &cob.PixCopiaECola, &cob.Revisao,
		&cob.CreatedAt, &cob.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrCobrancaNaoEncontrada
		}
		return nil, fmt.Errorf("buscando cobrança txid=%s: %w", txid, err)
	}

	cob.Status = domain.CobStatus(statusStr)
	cob.Valor.Original = domain.ValorCentavos(valorOriginal)
	cob.Calendar.Expiracao = int(calendarioExpiracao.Sub(cob.Calendar.Criacao).Seconds())
	return &cob, nil
}

// List busca cobranças com paginação e filtros.
func (r *CobRepo) List(ctx context.Context, filter domain.CobFilter) ([]domain.Cobranca, int, error) {
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}

	var total int
	err := r.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM cobrancas
		 WHERE ($1::timestamptz IS NULL OR created_at >= $1)
		 AND ($2::timestamptz IS NULL OR created_at <= $2)`,
		nullTime(filter.Inicio), nullTime(filter.Fim),
	).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("contando cobranças: %w", err)
	}

	rows, err := r.db.Query(ctx,
		`SELECT txid, chave_pix, valor_original::bigint, status,
		 calendario_criacao, calendario_expiracao, devedor_nome,
		 devedor_cpf, devedor_cnpj, solicitacao_pagador,
		 location_url, pix_copia_e_cola, revisao, created_at, updated_at
		 FROM cobrancas
		 WHERE ($3::timestamptz IS NULL OR created_at >= $3)
		 AND ($4::timestamptz IS NULL OR created_at <= $4)
		 ORDER BY created_at DESC
		 LIMIT $1 OFFSET $2`,
		filter.Limit, filter.Offset, nullTime(filter.Inicio), nullTime(filter.Fim),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("listando cobranças: %w", err)
	}
	defer rows.Close()

	var cobs []domain.Cobranca
	for rows.Next() {
		var cob domain.Cobranca
		var statusStr string
		var calendarioExpiracao time.Time
		var valorOriginal int64

		if err := rows.Scan(
			&cob.TxID, &cob.Chave, &valorOriginal, &statusStr,
			&cob.Calendar.Criacao, &calendarioExpiracao, &cob.Devedor.Nome,
			&cob.Devedor.CPF, &cob.Devedor.CNPJ, &cob.SolicitacaoPagador,
			&cob.Location, &cob.PixCopiaECola, &cob.Revisao,
			&cob.CreatedAt, &cob.UpdatedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("scaneando cobrança: %w", err)
		}
		cob.Status = domain.CobStatus(statusStr)
		cob.Valor.Original = domain.ValorCentavos(valorOriginal)
		cob.Calendar.Expiracao = int(calendarioExpiracao.Sub(cob.Calendar.Criacao).Seconds())
		cobs = append(cobs, cob)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterando cobranças: %w", err)
	}

	return cobs, total, nil
}
