-- 063_manager_commission_marts.sql
-- Totais mensais por grupo para comissão de gerentes (LSC).
-- Grão: empresa + filial + ano_mes + id_grupo + metric_kind
-- metric_kind: sales_base | stock_loss
-- Populado por publish na API (query slim → insert). Homolog e Prod compartilham CH.

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.manager_commission_group_month_rt (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    ano_mes           Int32 NOT NULL,
    id_grupo_produto  Int32 NOT NULL,
    metric_kind       LowCardinality(String) NOT NULL,
    valor             Decimal(18, 2) NOT NULL DEFAULT 0,
    qtd_itens         UInt64 NOT NULL DEFAULT 0,
    published_at      DateTime64(3, 'America/Sao_Paulo') NOT NULL DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, ano_mes, metric_kind, id_grupo_produto)
SETTINGS index_granularity = 8192;
