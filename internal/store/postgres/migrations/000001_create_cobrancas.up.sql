CREATE TABLE cobrancas (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    txid VARCHAR(35) NOT NULL UNIQUE,
    chave_pix VARCHAR(77) NOT NULL,
    valor_original NUMERIC(15,2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'ATIVA',
    calendario_criacao TIMESTAMPTZ NOT NULL,
    calendario_expiracao TIMESTAMPTZ NOT NULL,
    devedor_nome VARCHAR(100),
    devedor_cpf VARCHAR(14),
    devedor_cnpj VARCHAR(18),
    solicitacao_pagador TEXT,
    location_url TEXT,
    pix_copia_e_cola TEXT,
    revisao INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_valor_original CHECK (valor_original > 0),
    CONSTRAINT chk_devedor CHECK (devedor_cpf IS NOT NULL OR devedor_cnpj IS NOT NULL)
);

CREATE INDEX idx_cobrancas_chave_status ON cobrancas(chave_pix, status);
CREATE INDEX idx_cobrancas_created_at ON cobrancas(created_at);
