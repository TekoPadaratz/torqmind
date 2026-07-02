-- 046_mart_troca_forma_pgto.sql
-- Antifraude: mart realtime de troca de forma de pagamento (DE -> PARA).
--
-- Grain: 1 linha por troca (CONTROLE_TROCA_PGTO).
-- Sinal forte de fraude: forma JA RECEBIDA (dinheiro/cartao) trocada por forma
-- A RECEBER (prazo/convenio/cheque), indicando possivel desvio de caixa.
--
-- Fontes (torqmind_current):
--   stg_controle_troca_pgto  -> auditoria (quem/quando)            [grain]
--   stg_movlctoscancelados   -> lancamento cancelado = forma DE
--   stg_planodecontas        -> nome da forma/conta DE
--   stg_formas_pgto_slim     -> formas atuais do comprovante = PARA
--   payment_type_map         -> categoria (RECEBIDA / A_RECEBER)
--   stg_usuarios / stg_filiais -> rotulos operacionais
--
-- Idempotente (CREATE TABLE IF NOT EXISTS). ReplacingMergeTree por published_at.

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_troca_forma_pgto_rt (
    id_empresa            Int32 NOT NULL,
    id_filial             Int32 NOT NULL,
    filial_nome           String NOT NULL DEFAULT '',
    data_key              Int32 NOT NULL,
    dt                    Date NOT NULL,
    troca_id              Int64 NOT NULL,
    id_movlctoscancelados Int64 NOT NULL DEFAULT 0,
    referencia            Int64 NOT NULL DEFAULT 0,
    documento             String NOT NULL DEFAULT '',
    id_turno              Int32 NOT NULL DEFAULT 0,
    id_usuario            Int32 NOT NULL DEFAULT 0,
    nome_operador         String NOT NULL DEFAULT '',
    id_planodecontas_de   Int32 NOT NULL DEFAULT 0,
    forma_de              String NOT NULL DEFAULT '',
    categoria_de          LowCardinality(String) NOT NULL DEFAULT '',
    forma_para            String NOT NULL DEFAULT '',
    categoria_para        LowCardinality(String) NOT NULL DEFAULT '',
    valor                 Decimal(18,2) NOT NULL DEFAULT 0,
    data_troca_ts         Nullable(DateTime64(6, 'America/Sao_Paulo')),
    hora                  UInt8 NOT NULL DEFAULT 0,
    is_suspeita           UInt8 NOT NULL DEFAULT 0,
    score_risco           Int32 NOT NULL DEFAULT 0,
    reasons               String NOT NULL DEFAULT '{}',
    published_at          DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, data_key, troca_id)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;
