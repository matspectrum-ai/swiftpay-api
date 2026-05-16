ALTER TABLE outbox_messages DROP COLUMN IF EXISTS claimed_at;
ALTER TABLE outbox_messages DROP COLUMN IF EXISTS claimed_by;
ALTER TABLE outbox_messages DROP COLUMN IF EXISTS lease_timeout;
DROP VIEW IF EXISTS outbox_pending;
