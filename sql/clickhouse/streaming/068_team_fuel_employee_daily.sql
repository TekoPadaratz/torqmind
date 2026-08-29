-- Abastecimentos por funcionário (litros combustível) — grão dia × filial × funcionário × produto
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.team_fuel_employee_daily_rt (
    id_empresa        Int32 NOT NULL,
    id_filial         Int32 NOT NULL,
    data_key          Int32 NOT NULL,
    dt                Date NOT NULL,
    id_funcionario    Int32 NOT NULL,
    id_produto        Int32 NOT NULL,
    nome_produto      String DEFAULT '',
    nome_grupo        String DEFAULT '',
    litros            Decimal(18, 3) DEFAULT 0,
    faturamento       Decimal(18, 2) DEFAULT 0,
    published_at      DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(published_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (id_empresa, id_filial, data_key, id_funcionario, id_produto);
