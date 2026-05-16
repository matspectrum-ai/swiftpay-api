ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS claimed_by VARCHAR(64);
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS lease_timeout INTEGER NOT NULL DEFAULT 300;

CREATE OR REPLACE VIEW outbox_pending AS
SELECT * FROM outbox_messages
WHERE published_at IS NULL
  AND attempts < max_attempts
  AND (claimed_at IS NULL OR claimed_at + (lease_timeout || ' seconds')::INTERVAL < NOW());
