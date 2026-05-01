-- TorqMind Event-Driven Streaming: Realtime Mart Layer
-- Database and core tables for realtime analytics marts.
-- Fed by the Mart Builder from torqmind_current (deduplicated CDC state).
-- All tables use ReplacingMergeTree for idempotent refresh.

CREATE DATABASE IF NOT EXISTS torqmind_mart_rt;

-- ============================================================
-- PUBLICATION TRACKING
-- ============================================================
CREATE TABLE IF NOT EXISTS torqmind_mart_rt.mart_publication_log (
    mart_name         LowCardinality(String) NOT NULL,
    id_empresa        Int32 NOT NULL,
    window_start      Date NOT NULL,
    window_end        Date NOT NULL,
    rows_written      UInt64 NOT NULL DEFAULT 0,
    duration_ms       UInt64 NOT NULL DEFAULT 0,
    published_at      DateTime64(6, 'UTC') NOT NULL DEFAULT now64(6)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(published_at)
ORDER BY (mart_name, id_empresa, published_at)
TTL toDateTime(published_at) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
