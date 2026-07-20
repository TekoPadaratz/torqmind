-- 053_bi_hotpath_ch_marts.sql
-- Hot-path BI: espelhos CH das marts PG ainda lidas pelo front.
-- Mash continua no PG; publish → estas tabelas; API lê SÓ daqui.
-- Idempotente.

CREATE DATABASE IF NOT EXISTS torqmind_mart_rt;
CREATE DATABASE IF NOT EXISTS torqmind_mart;

-- Orçamento: realizado por conta/mês
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_despesa_conta_mensal (
    id_empresa       Int32,
    id_filial        Int32,
    id_plano_conta   Int32,
    ano              Int32,
    mes              Int32,
    codigo           String DEFAULT '',
    nome_conta       String DEFAULT '',
    valor_realizado  Decimal(18, 2) DEFAULT 0,
    qtd              Int32 DEFAULT 0,
    published_at     DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, ano, mes, id_plano_conta)
PARTITION BY ano
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_plano_contas_gerencial (
    id_empresa       Int32,
    id_filial        Int32,
    id_plano_conta   Int32,
    codigo           String DEFAULT '',
    nome_conta       String DEFAULT '',
    published_at     DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_plano_conta)
SETTINGS index_granularity = 8192;

-- Cheques (Financeiro + Solvência)
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_cheques_pendentes (
    id_empresa        Int32,
    id_filial         Int32,
    id_db             Int32 DEFAULT 0,
    id_cheque         Int32,
    id_entidade       Int32 DEFAULT 0,
    cliente_nome      String DEFAULT '',
    cpf               String DEFAULT '',
    valor             Decimal(18, 2) DEFAULT 0,
    dt_recebido       Nullable(Date),
    dt_vencimento     Nullable(Date),
    dt_compensado     Nullable(Date),
    situacao_cheque   Int16 DEFAULT 0,
    avista            UInt8 DEFAULT 0,
    motivo_devolucao  String DEFAULT '',
    status_cheque     LowCardinality(String) DEFAULT 'a_compensar',
    banco             String DEFAULT '',
    agencia           String DEFAULT '',
    nroconta          String DEFAULT '',
    numero            String DEFAULT '',
    published_at      DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_db, id_cheque)
SETTINGS index_granularity = 8192;

-- Ticket combustível (Vendas)
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_ticket_combustivel_diaria (
    id_empresa          Int32,
    id_filial           Int32,
    data_ref            Date,
    valor_total         Decimal(18, 2) DEFAULT 0,
    litros_total        Decimal(18, 3) DEFAULT 0,
    qtd_abastecimentos  Int32 DEFAULT 0,
    ticket_medio        Decimal(18, 2) DEFAULT 0,
    published_at        DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_ref)
PARTITION BY toYYYYMM(data_ref)
SETTINGS index_granularity = 8192;

-- Solvência itens + bancos + liquidez (detalhada)
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_solvencia_item (
    id_empresa   Int32,
    id_filial    Int32,
    grupo        LowCardinality(String),
    secao        LowCardinality(String),
    item_label   String DEFAULT '',
    valor        Decimal(18, 2) DEFAULT 0,
    qtd          Decimal(18, 4) DEFAULT 0,
    origem       LowCardinality(String) DEFAULT 'auto',
    ordem        Int32 DEFAULT 0,
    published_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, grupo, secao, item_label)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_solvencia_banco_conta (
    id_empresa          Int32,
    id_filial           Int32,
    ano_mes             Int32,
    id_contasbancarias  Int32,
    banco_nome          String DEFAULT '',
    agencia             String DEFAULT '',
    nro_conta           String DEFAULT '',
    descricao           String DEFAULT '',
    ativo               UInt8 DEFAULT 1,
    saldo               Decimal(18, 2) DEFAULT 0,
    published_at        DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, ano_mes, id_contasbancarias)
PARTITION BY ano_mes
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_liquidez_solvencia (
    id_empresa                  Int32,
    id_filial                   Int32,
    ano_mes                     Int32,
    passivo_contas_pagar        Decimal(18, 2) DEFAULT 0,
    passivo_qtd_titulos         Int32 DEFAULT 0,
    passivo_vencido             Decimal(18, 2) DEFAULT 0,
    ativo_caixa                 Decimal(18, 2) DEFAULT 0,
    ativo_banco                 Decimal(18, 2) DEFAULT 0,
    ativo_cartoes               Decimal(18, 2) DEFAULT 0,
    ativo_cheques               Decimal(18, 2) DEFAULT 0,
    ativo_estoque               Decimal(18, 2) DEFAULT 0,
    ativo_estoque_combustivel   Decimal(18, 2) DEFAULT 0,
    ativo_estoque_loja          Decimal(18, 2) DEFAULT 0,
    ativo_cartoes_credito       Decimal(18, 2) DEFAULT 0,
    ativo_cartoes_debito        Decimal(18, 2) DEFAULT 0,
    tem_ativo_dados             UInt8 DEFAULT 0,
    estoque_combustivel_medido  UInt8 DEFAULT 0,
    estoque_data_leitura        Nullable(Date),
    published_at                DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, ano_mes)
PARTITION BY ano_mes
SETTINGS index_granularity = 8192;

-- Antifraude: crédito cliente (MOVCREDITO) — sem CDC; mash PG→CH
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_fraud_credito_cliente_mov (
    id_empresa     Int32,
    id_filial      Int32,
    id_db          Int32 DEFAULT 0,
    id_mov         Int64,
    id_entidade    Int32,
    id_usuario     Int32 DEFAULT 0,
    operador       String DEFAULT '',
    data_dia       Date,
    data_raw       String DEFAULT '',
    entradas       Decimal(18, 2) DEFAULT 0,
    saidas         Decimal(18, 2) DEFAULT 0,
    historico      String DEFAULT '',
    referencia     String DEFAULT '',
    manual_suspeita UInt8 DEFAULT 0,
    published_at   DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_entidade, id_mov)
PARTITION BY toYYYYMM(data_dia)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_fraud_credito_cliente_saldo (
    id_empresa   Int32,
    id_filial    Int32,
    id_entidade  Int32,
    saldo        Decimal(18, 2) DEFAULT 0,
    published_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_entidade)
SETTINGS index_granularity = 8192;

-- Solvência: a prazo por mês (materializado a partir de stg_contasreceber no CH)
-- Evita ler STG no hot path da tela Solvência detalhada (ativos_do_mes).
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_solvencia_aprazo_mes (
    id_empresa   Int32,
    id_filial    Int32,
    ano_mes      Int32,
    valor        Decimal(18, 2) DEFAULT 0,
    qtd          Int32 DEFAULT 0,
    published_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, ano_mes)
PARTITION BY ano_mes
SETTINGS index_granularity = 8192;
