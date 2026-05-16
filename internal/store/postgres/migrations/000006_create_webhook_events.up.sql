CREATE TABLE webhook_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    e2eid VARCHAR(64) NOT NULL,
    chave_pix VARCHAR(77) NOT NULL,
    payload JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_webhook_event UNIQUE(e2eid, chave_pix)
);

CREATE INDEX idx_webhook_events_e2eid ON webhook_events(e2eid);
