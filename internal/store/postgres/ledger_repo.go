package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type LedgerEvent struct {
	ID              string
	EventType       string
	AggregateType   string
	AggregateID     string
	CorrelationID   string
	RequestID       string
	TxID            string
	EndToEndID           string
	PreviousState   json.RawMessage
	NextState       json.RawMessage
	Changes         json.RawMessage
	OperationSource string
}

type LedgerRepo struct {
	db *pgxpool.Pool
}

func NewLedgerRepo(db *pgxpool.Pool) *LedgerRepo {
	return &LedgerRepo{db: db}
}

func (r *LedgerRepo) Append(ctx context.Context, tx pgx.Tx, ev *LedgerEvent) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO ledger_events (event_type, aggregate_type, aggregate_id, correlation_id, request_id, txid, e2eid, previous_state, next_state, changes, operation_source)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		ev.EventType, ev.AggregateType, ev.AggregateID, ev.CorrelationID, ev.RequestID,
		ev.TxID, ev.EndToEndID, ev.PreviousState, ev.NextState, ev.Changes, ev.OperationSource,
	)
	if err != nil {
		return fmt.Errorf("append ledger event: %w", err)
	}
	return nil
}
