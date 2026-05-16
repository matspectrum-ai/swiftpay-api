CREATE TABLE idempotency_keys (
    idempotency_key VARCHAR(64) NOT NULL,
    endpoint_path VARCHAR(255) NOT NULL,
    request_hash VARCHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'in_progress',
    response_status INTEGER,
    response_body JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '24 hours'),
    PRIMARY KEY (idempotency_key, endpoint_path)
);

CREATE INDEX idx_idempotency_status ON idempotency_keys(status);
CREATE INDEX idx_idempotency_expires ON idempotency_keys(expires_at);
