CREATE TABLE reconciliation_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    e2eid VARCHAR(64),
    local_valor NUMERIC(15,2),
    psp_valor NUMERIC(15,2),
    local_horario TIMESTAMPTZ,
    psp_horario TIMESTAMPTZ,
    tipo_discrepancia VARCHAR(50) NOT NULL,
    resolvido BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
