-- ============================================================
-- 092 — Identidade visual por empresa (branding)
-- ============================================================
-- Metadados de branding por empresa. Os arquivos ficam em storage
-- persistente (volume torqmind_branding montado na API); aqui guardamos
-- apenas caminho relativo, mime, tamanho e uma versao (hash curto) para
-- cache-busting. id_empresa = chave; empresa sem linha usa o padrao TorqMind.
-- ============================================================

CREATE TABLE IF NOT EXISTS app.company_branding (
    id_empresa            INTEGER PRIMARY KEY
        REFERENCES app.tenants(id_empresa) ON DELETE CASCADE,
    background_image_path TEXT,
    background_mime_type  TEXT,
    background_file_size  INTEGER,
    background_version    TEXT,
    logo_image_path       TEXT,
    logo_mime_type        TEXT,
    logo_file_size        INTEGER,
    logo_version          TEXT,
    updated_by            UUID,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE app.company_branding IS
    'Identidade visual por empresa (fundo e logo). Arquivos em storage persistente; aqui so metadados. Empresa sem linha usa o padrao TorqMind.';
