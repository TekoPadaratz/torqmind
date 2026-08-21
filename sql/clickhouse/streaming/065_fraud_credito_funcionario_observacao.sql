-- 065: OBS (Observações) do CONTASRECEBER no detalhe de uso crédito funcionário.
-- Idempotente. Homolog/Prod compartilham o CH — aplicar uma vez na analytics.

ALTER TABLE torqmind_mart_rt.mart_fraud_credito_funcionario_uso
    ADD COLUMN IF NOT EXISTS observacao String DEFAULT '';
