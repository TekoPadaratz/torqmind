-- TorqMind Event-Driven Streaming: Raw events layer
-- Idempotent log of all Debezium CDC events.
-- ReplacingMergeTree deduplicates by (topic, kafka_partition, kafka_offset).
-- TTL 90 days; adjust in production.

CREATE TABLE IF NOT EXISTS torqmind_raw.cdc_events (
    topic             LowCardinality(String)  NOT NULL,
    kafka_partition   Int32                   NOT NULL,
    kafka_offset      Int64                   NOT NULL,
    op                LowCardinality(String)  NOT NULL COMMENT 'c=create, u=update, d=delete, r=read(snapshot)',
    source_ts_ms      Int64                   NOT NULL COMMENT 'Debezium source.ts_ms',
    table_schema      LowCardinality(String)  NOT NULL DEFAULT '',
    table_name        LowCardinality(String)  NOT NULL DEFAULT '',
    id_empresa        Int32                   NOT NULL DEFAULT 0,
    data_key          Int32                   NOT NULL DEFAULT 0,
    key_json          String                  NOT NULL DEFAULT '{}',
    before_json       String                  NOT NULL DEFAULT '{}',
    after_json        String                  NOT NULL DEFAULT '{}',
    ingested_at       DateTime64(6, 'UTC')    NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (topic, kafka_partition, kafka_offset)
PARTITION BY toYYYYMM(toDate(fromUnixTimestamp64Milli(source_ts_ms)))
TTL toDate(fromUnixTimestamp64Milli(source_ts_ms)) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
