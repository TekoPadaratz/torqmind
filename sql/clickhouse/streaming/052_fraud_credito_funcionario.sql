-- 052_fraud_credito_funcionario.sql
-- Antifraude: crédito/vale de funcionário (ENTIDADES.LIMITE + LIMITE_VALE).
-- Mash pesado roda no PG (etl.refresh_fraud_credito_funcionario); API lê SÓ daqui.
-- Idempotente.

CREATE DATABASE IF NOT EXISTS torqmind_mart_rt;

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_fraud_credito_funcionario_resumo (
    id_empresa           Int32,
    id_funcionario       Int32,
    ano_mes              Int32,
    id_filial_ref        Int32 DEFAULT 0,
    id_entidade          Int32 DEFAULT 0,
    nome_funcionario     String DEFAULT '',
    cpf                  String DEFAULT '',
    ativo                UInt8 DEFAULT 1,
    limite_prazo         Decimal(18, 2) DEFAULT 0,
    limite_vale          Decimal(18, 2) DEFAULT 0,
    limite_total         Decimal(18, 2) DEFAULT 0,
    vales_cadastro       Decimal(18, 2) DEFAULT 0,
    usado_prazo          Decimal(18, 2) DEFAULT 0,
    usado_vale           Decimal(18, 2) DEFAULT 0,
    usado_mes            Decimal(18, 2) DEFAULT 0,
    saldo_prazo          Decimal(18, 2) DEFAULT 0,
    saldo_vale           Decimal(18, 2) DEFAULT 0,
    saldo_restante       Decimal(18, 2) DEFAULT 0,
    qtd_usos_mes         Int32 DEFAULT 0,
    max_usos_mesmo_dia   Int32 DEFAULT 0,
    status               LowCardinality(String) DEFAULT 'Normal',
    motivos              String DEFAULT '[]',
    published_at         DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, ano_mes, id_funcionario)
PARTITION BY ano_mes
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_fraud_credito_funcionario_uso (
    id_empresa           Int32,
    id_funcionario       Int32,
    ano_mes              Int32,
    id_filial            Int32,
    id_entidade          Int32 DEFAULT 0,
    id_contasreceber     Int32,
    id_comprovante       Int64 DEFAULT 0,
    nro_cupom            String DEFAULT '',
    nro_documento        String DEFAULT '',
    tipo_uso             LowCardinality(String) DEFAULT 'prazo',
    dt_evento            Nullable(DateTime64(3, 'America/Sao_Paulo')),
    valor                Decimal(18, 2) DEFAULT 0,
    id_usuario_caixa     Int32 DEFAULT 0,
    operador_caixa       String DEFAULT '',
    historico            String DEFAULT '',
    observacao           String DEFAULT '',
    atipico              UInt8 DEFAULT 0,
    published_at         DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, ano_mes, id_funcionario, id_filial, id_contasreceber)
PARTITION BY ano_mes
SETTINGS index_granularity = 8192;
