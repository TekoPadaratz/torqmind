-- 064_mart_finance_titles_nro_documento.sql
-- Nº do documento do título (NRODOC/DOCUMENTO do Xpert) para busca e baixa no posto.

ALTER TABLE torqmind_mart_rt.mart_finance_titles_rt
    ADD COLUMN IF NOT EXISTS nro_documento String DEFAULT '';
