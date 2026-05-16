package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matspectrum/swiftpay-api/internal/domain"
)

// PixRepo gerencia persistência de Pix recebidos.
type PixRepo struct {
	db *pgxpool.Pool
}

// NewPixRepo cria um novo repositório de Pix.
func NewPixRepo(db *pgxpool.Pool) *PixRepo {
	return &PixRepo{db: db}
}

// Create insere um Pix recebido (dentro de transação).
func (r *PixRepo) Create(ctx context.Context, tx pgx.Tx, pix *domain.PixRecebido) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO pix_recebidos (e2eid, txid, chave_pix, valor, horario_liquidacao,
		 pagador_nome, pagador_cpf, pagador_cnpj, info_pagador)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		pix.E2EID, pix.TxID, pix.Chave, int64(pix.ValorCentavos),
		pix.HorarioLiquidacao, pix.PagadorNome, pix.PagadorCPF,
		pix.PagadorCNPJ, pix.InfoPagador,
	)
	if err != nil {
		return fmt.Errorf("inserindo pix e2eid=%s: %w", pix.E2EID, err)
	}
	return nil
}

// GetByE2EID busca Pix por e2eid.
func (r *PixRepo) GetByE2EID(ctx context.Context, e2eid string) (*domain.PixRecebido, error) {
	var pix domain.PixRecebido
	var valorCentavos int64

	err := r.db.QueryRow(ctx,
		`SELECT e2eid, txid, chave_pix, valor::bigint, horario_liquidacao,
		 pagador_nome, pagador_cpf, pagador_cnpj, info_pagador, created_at
		 FROM pix_recebidos WHERE e2eid = $1`, e2eid,
	).Scan(
		&pix.E2EID, &pix.TxID, &pix.Chave, &valorCentavos,
		&pix.HorarioLiquidacao, &pix.PagadorNome, &pix.PagadorCPF,
		&pix.PagadorCNPJ, &pix.InfoPagador, &pix.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrPixNaoEncontrado
		}
		return nil, fmt.Errorf("buscando pix e2eid=%s: %w", e2eid, err)
	}

	pix.ValorCentavos = domain.ValorCentavos(valorCentavos)
	return &pix, nil
}

// List busca Pix recebidos com paginação e filtros.
func (r *PixRepo) List(ctx context.Context, filter domain.PixFilter) ([]domain.PixRecebido, int, error) {
	if filter.Limit <= 0 || filter.Limit > 100 {
		filter.Limit = 20
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}

	var total int
	err := r.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM pix_recebidos
		 WHERE ($1::timestamptz IS NULL OR horario_liquidacao >= $1)
		 AND ($2::timestamptz IS NULL OR horario_liquidacao <= $2)
		 AND ($3::varchar IS NULL OR txid = $3)
		 AND ($4::varchar IS NULL OR chave_pix = $4)`,
		filter.Inicio, filter.Fim, filter.TxID, filter.Chave,
	).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("contando pix: %w", err)
	}

	rows, err := r.db.Query(ctx,
		`SELECT e2eid, txid, chave_pix, valor::bigint, horario_liquidacao,
		 pagador_nome, pagador_cpf, pagador_cnpj, info_pagador, created_at
		 FROM pix_recebidos
		 WHERE ($3::timestamptz IS NULL OR horario_liquidacao >= $3)
		 AND ($4::timestamptz IS NULL OR horario_liquidacao <= $4)
		 AND ($5::varchar IS NULL OR txid = $5)
		 AND ($6::varchar IS NULL OR chave_pix = $6)
		 ORDER BY horario_liquidacao DESC
		 LIMIT $1 OFFSET $2`,
		filter.Limit, filter.Offset,
		filter.Inicio, filter.Fim,
		filter.TxID, filter.Chave,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("listando pix: %w", err)
	}
	defer rows.Close()

	var pixs []domain.PixRecebido
	for rows.Next() {
		var pix domain.PixRecebido
		var valorCentavos int64
		if err := rows.Scan(
			&pix.E2EID, &pix.TxID, &pix.Chave, &valorCentavos,
			&pix.HorarioLiquidacao, &pix.PagadorNome, &pix.PagadorCPF,
			&pix.PagadorCNPJ, &pix.InfoPagador, &pix.CreatedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("scaneando pix: %w", err)
		}
		pix.ValorCentavos = domain.ValorCentavos(valorCentavos)
		pixs = append(pixs, pix)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterando pix: %w", err)
	}

	return pixs, total, nil
}

// CreateDevolucao insere uma devolução na tabela de devoluções.
func (r *PixRepo) CreateDevolucao(ctx context.Context, tx pgx.Tx, dev *domain.Devolucao) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO devolucoes (external_id, e2eid, valor, status, horario)
		 VALUES ($1, $2, $3, $4, $5)`,
		dev.ID, dev.E2EID, int64(dev.Valor), dev.Status, dev.Horario,
	)
	if err != nil {
		return fmt.Errorf("inserindo devolucao id=%s: %w", dev.ID, err)
	}
	return nil
}

// ListDevolucoes lista devoluções por e2eid.
func (r *PixRepo) ListDevolucoes(ctx context.Context, e2eid string) ([]domain.Devolucao, error) {
	rows, err := r.db.Query(ctx,
		`SELECT external_id, e2eid, valor::bigint, status, horario, created_at
		 FROM devolucoes WHERE e2eid = $1 ORDER BY created_at DESC`, e2eid,
	)
	if err != nil {
		return nil, fmt.Errorf("listando devolucoes e2eid=%s: %w", e2eid, err)
	}
	defer rows.Close()

	var devs []domain.Devolucao
	for rows.Next() {
		var dev domain.Devolucao
		var valorCentavos int64
		if err := rows.Scan(
			&dev.ID, &dev.E2EID, &valorCentavos,
			&dev.Status, &dev.Horario, &dev.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scaneando devolucao: %w", err)
		}
		dev.Valor = domain.ValorCentavos(valorCentavos)
		devs = append(devs, dev)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterando devolucoes: %w", err)
	}

	return devs, nil
}
