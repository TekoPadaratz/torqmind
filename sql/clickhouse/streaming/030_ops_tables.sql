-- TorqMind Event-Driven Streaming: Operations layer
-- Tables for CDC consumer state, lag monitoring, and error tracking.
-- All columns aligned with clickhouse_writer.py inserts.

CREATE TABLE IF NOT EXISTS torqmind_ops.consumer_offsets (
    consumer_group    LowCardinality(String) NOT NULL,
    topic             LowCardinality(String) NOT NULL,
    kafka_partition   UInt32 NOT NULL,
    committed_offset  UInt64 NOT NULL,
    committed_at      DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(committed_at)
ORDER BY (consumer_group, topic, kafka_partition)
SETTINGS index_granularity = 8192;

-- Writer inserts: (table_schema, table_name, id_empresa, last_source_ts_ms, last_op, events_total)
-- last_event_at and updated_at are DEFAULT-filled.
CREATE TABLE IF NOT EXISTS torqmind_ops.cdc_table_state (
    table_schema      LowCardinality(String) NOT NULL,
    table_name        LowCardinality(String) NOT NULL,
    id_empresa        Int32 NOT NULL DEFAULT 0,
    last_source_ts_ms Int64 NOT NULL DEFAULT 0,
    last_op           LowCardinality(String) NOT NULL DEFAULT '',
    events_total      UInt64 NOT NULL DEFAULT 0,
    last_event_at     DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6),
    updated_at        DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (table_schema, table_name, id_empresa)
SETTINGS index_granularity = 8192;

-- Writer inserts: (consumer_group, topic, kafka_partition, kafka_offset,
--                  table_schema, table_name, error_type, error_message, event_payload)
-- id and created_at are DEFAULT-filled.
CREATE TABLE IF NOT EXISTS torqmind_ops.cdc_errors (
    id                UUID DEFAULT generateUUIDv4(),
    consumer_group    LowCardinality(String) NOT NULL DEFAULT '',
    topic             LowCardinality(String) NOT NULL DEFAULT '',
    kafka_partition   UInt32 NOT NULL DEFAULT 0,
    kafka_offset      UInt64 NOT NULL DEFAULT 0,
    table_schema      LowCardinality(String) NOT NULL DEFAULT '',
    table_name        LowCardinality(String) NOT NULL DEFAULT '',
    error_type        LowCardinality(String) NOT NULL DEFAULT 'UNKNOWN',
    error_message     String NOT NULL DEFAULT '',
    event_payload     String NOT NULL DEFAULT '',
    created_at        DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (consumer_group, created_at)
TTL toDateTime(created_at) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS torqmind_ops.cdc_lag (
    consumer_group    LowCardinality(String) NOT NULL,
    topic             LowCardinality(String) NOT NULL,
    kafka_partition   UInt32 NOT NULL,
    current_offset    UInt64 NOT NULL DEFAULT 0,
    end_offset        UInt64 NOT NULL DEFAULT 0,
    lag               UInt64 NOT NULL DEFAULT 0,
    measured_at       DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(measured_at)
ORDER BY (consumer_group, topic, kafka_partition, measured_at)
TTL toDateTime(measured_at) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- Summary view for monitoring dashboards
CREATE OR REPLACE VIEW torqmind_ops.cdc_status_summary AS
SELECT
    ts.table_schema,
    ts.table_name,
    ts.id_empresa,
    ts.events_total,
    ts.last_source_ts_ms,
    ts.last_event_at,
    ts.last_op
FROM torqmind_ops.cdc_table_state AS ts
FINAL;
