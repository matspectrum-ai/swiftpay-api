CREATE TABLE leader_leases (
    lock_id BIGINT NOT NULL,
    instance_id VARCHAR(64) NOT NULL,
    epoch BIGINT NOT NULL DEFAULT 1,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (lock_id)
);

CREATE INDEX idx_leader_leases_expires ON leader_leases(expires_at);
