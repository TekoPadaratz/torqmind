-- TorqMind Event-Driven Streaming: Database layer
-- Creates separate databases for raw events, current state, and operations.
-- Safe to re-run (IF NOT EXISTS).

CREATE DATABASE IF NOT EXISTS torqmind_raw;
CREATE DATABASE IF NOT EXISTS torqmind_current;
CREATE DATABASE IF NOT EXISTS torqmind_ops;
-- torqmind_mart already exists from legacy; streaming pilot marts will live there too.
