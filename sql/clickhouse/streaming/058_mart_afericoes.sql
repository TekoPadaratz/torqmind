-- 058_mart_afericoes.sql
-- Aferições operacionais de bico (mash STG → CH) para tela Operação/combustível.

CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_afericoes_rt (
    id_empresa       Int32,
    id_filial        Int32,
    id_afericao      Int32,
    id_bico          Int32 DEFAULT 0,
    id_turno         Int32 DEFAULT 0,
    turno_operacional Int32 DEFAULT 0,
    bico_label       String DEFAULT '',
    produto_nome     String DEFAULT '',
    qtde_l           Decimal(18, 3) DEFAULT 0,
    dia              Date,
    dt_evento        DateTime64(3, 'America/Sao_Paulo'),
    id_usuario       Int32 DEFAULT 0,
    id_usuario_lib   Int32 DEFAULT 0,
    operador_nome    String DEFAULT '',
    liberador_nome   String DEFAULT '',
    published_at     DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(published_at)
ORDER BY (id_empresa, id_filial, id_afericao)
PARTITION BY toYYYYMM(dia)
SETTINGS index_granularity = 8192;
