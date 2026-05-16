CREATE TABLE outbox_dead_letter (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_id UUID NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    attempts INTEGER NOT NULL,
    last_error TEXT,
    moved_at TIMESTAMPTZ NOT NULL,
    reprocessed_at TIMESTAMPTZ,
    reprocessed_by VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_deadletter_original ON outbox_dead_letter(original_id);
CREATE INDEX idx_deadletter_moved ON outbox_dead_letter(moved_at);
