-- 066_fraud_credito_funcionario_aberto_pago.sql
-- Antifraude crédito funcionário: saldo aberto × contas a receber + flag pago/aberto.
-- Idempotente (ADD COLUMN IF NOT EXISTS).

ALTER TABLE torqmind_mart_rt.mart_fraud_credito_funcionario_resumo
    ADD COLUMN IF NOT EXISTS usado_geral Decimal(18, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS pago_mes Decimal(18, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS saldo_aberto_geral Decimal(18, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS saldo_aberto_mes Decimal(18, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS saldo_aberto_prazo Decimal(18, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS qtd_aberto_vencido Int32 DEFAULT 0;

ALTER TABLE torqmind_mart_rt.mart_fraud_credito_funcionario_uso
    ADD COLUMN IF NOT EXISTS vlr_pago Decimal(18, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS saldo_aberto Decimal(18, 2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS dt_vencimento Nullable(DateTime64(3, 'America/Sao_Paulo')),
    ADD COLUMN IF NOT EXISTS dt_pagamento Nullable(DateTime64(3, 'America/Sao_Paulo')),
    ADD COLUMN IF NOT EXISTS situacao LowCardinality(String) DEFAULT 'aberto',
    ADD COLUMN IF NOT EXISTS grupo_lista LowCardinality(String) DEFAULT '';
