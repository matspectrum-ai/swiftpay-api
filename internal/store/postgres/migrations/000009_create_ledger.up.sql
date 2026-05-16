CREATE TABLE ledger_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(100) NOT NULL,
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    correlation_id VARCHAR(64),
    request_id VARCHAR(64),
    txid VARCHAR(35),
    e2eid VARCHAR(64),
    previous_state JSONB,
    next_state JSONB,
    changes JSONB,
    operation_source VARCHAR(50) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ledger_aggregate ON ledger_events(aggregate_type, aggregate_id);
CREATE INDEX idx_ledger_txid ON ledger_events(txid) WHERE txid IS NOT NULL;
CREATE INDEX idx_ledger_e2eid ON ledger_events(e2eid) WHERE e2eid IS NOT NULL;
CREATE INDEX idx_ledger_correlation ON ledger_events(correlation_id) WHERE correlation_id IS NOT NULL;
CREATE INDEX idx_ledger_occurred ON ledger_events(occurred_at);
