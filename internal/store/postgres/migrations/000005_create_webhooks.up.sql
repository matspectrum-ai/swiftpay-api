CREATE TABLE webhooks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chave_pix VARCHAR(77) NOT NULL UNIQUE,
    webhook_url VARCHAR(2048) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ATIVO',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_webhooks_chave ON webhooks(chave_pix);
