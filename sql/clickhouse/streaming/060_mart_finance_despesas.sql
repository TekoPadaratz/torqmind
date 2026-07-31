-- 060_mart_finance_despesas.sql
-- Despesas operacionais (contas a pagar × plano de contas DRE).
-- Grão: empresa / filial / db / título CAP.
-- Status operacional: pago | a_vencer | vencido (mesma lógica de títulos).
-- Sem PARTITION BY mês: janela ampla atravessa vários YYYYMM.

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_finance_despesas_rt (
    id_empresa              Int32,
    id_filial               Int32,
    filial_nome             String DEFAULT '',
    id_titulo               Int64,
    id_db                   Int32 DEFAULT 0,
    id_planodecontas        Int32,
    codigo_plano            String DEFAULT '',
    nome_plano              String DEFAULT '',
    classificacao_gerencial LowCardinality(String) DEFAULT '',
    entra_custo_operacional UInt8 DEFAULT 0,
    historico               String DEFAULT '',
    documento               String DEFAULT '',
    dt_vencimento           Date,
    dt_pagamento            Nullable(Date),
    valor                   Decimal(18, 2) DEFAULT 0,
    valor_pago              Decimal(18, 2) DEFAULT 0,
    valor_aberto            Decimal(18, 2) DEFAULT 0,
    status                  LowCardinality(String) DEFAULT 'a_vencer',
    ano_mes_vencimento      Int32 DEFAULT 0,
    published_at            DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_db, id_titulo)
SETTINGS index_granularity = 8192;

-- Cadastro de funcionários ativos (base para custo fully-loaded).
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_team_employees_rt (
    id_empresa       Int32,
    id_filial        Int32,
    filial_nome      String DEFAULT '',
    id_funcionario   Int64,
    id_usuario       Int32 DEFAULT 0,
    nome             String DEFAULT '',
    funcao           String DEFAULT '',
    ativo            UInt8 DEFAULT 1,
    salario_bruto    Decimal(18, 2) DEFAULT 0,
    salario_total    Decimal(18, 2) DEFAULT 0,
    vales            Decimal(18, 2) DEFAULT 0,
    horas_extras     Decimal(18, 2) DEFAULT 0,
    published_at     DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_funcionario)
SETTINGS index_granularity = 8192;
