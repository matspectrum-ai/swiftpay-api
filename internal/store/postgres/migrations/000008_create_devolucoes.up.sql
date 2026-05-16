CREATE TABLE devolucoes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(64) NOT NULL,
    e2eid VARCHAR(64) NOT NULL,
    valor NUMERIC(15,2) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'EM_PROCESSAMENTO',
    horario TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_devolucao_pix FOREIGN KEY (e2eid) REFERENCES pix_recebidos(e2eid)
);

CREATE INDEX idx_devolucoes_e2eid ON devolucoes(e2eid);
