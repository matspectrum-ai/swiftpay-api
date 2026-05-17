CREATE TABLE settlement_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_date DATE NOT NULL,
    total_pix_received BIGINT NOT NULL DEFAULT 0,
    total_amount_cents BIGINT NOT NULL DEFAULT 0,
    total_discrepancies INT NOT NULL DEFAULT 0,
    reconciliation_completed BOOLEAN NOT NULL DEFAULT FALSE,
    snapshot_data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(snapshot_date)
);
