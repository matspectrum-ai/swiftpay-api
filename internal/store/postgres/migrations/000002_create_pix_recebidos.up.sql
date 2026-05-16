CREATE TABLE pix_recebidos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    e2eid VARCHAR(64) NOT NULL UNIQUE,
    txid VARCHAR(35),
    chave_pix VARCHAR(77) NOT NULL,
    valor NUMERIC(15,2) NOT NULL,
    horario_liquidacao TIMESTAMPTZ NOT NULL,
    pagador_nome VARCHAR(100),
    pagador_cpf VARCHAR(14),
    pagador_cnpj VARCHAR(18),
    info_pagador TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_pix_txid FOREIGN KEY (txid) REFERENCES cobrancas(txid)
);

CREATE INDEX idx_pix_chave ON pix_recebidos(chave_pix);
CREATE INDEX idx_pix_horario ON pix_recebidos(horario_liquidacao);
CREATE INDEX idx_pix_txid ON pix_recebidos(txid);
