# TorqMind Realtime Operations Runbook

## Quick Reference

| Command | What it does |
|---------|--------------|
| `make streaming-init-mart-rt` | Applies tracked `040/041` mart_rt DDL and verifies mandatory tables |
| `make realtime-cutover` | Full guarded DW-origin cutover candidate |
| `make realtime-validate` | Blocking parity/API validation with fallback disabled |
| `make realtime-backfill` | Rebuilds mart_rt from `torqmind_current` |
| `make realtime-e2e-smoke` | Inserts a DW fixture and verifies raw/current/MartBuilder/API |
| `make realtime-rollback` | Sets `USE_REALTIME_MARTS=false` and restarts API |
| `make streaming-status` | Shows Debezium/Redpanda/CDC status |

## Current Architecture

```text
Agent/API -> PostgreSQL STG -> ETL STG->DW -> Debezium(dw.*)
  -> Redpanda -> CDC Consumer -> ClickHouse torqmind_raw/torqmind_current
  -> MartBuilder -> ClickHouse torqmind_mart_rt -> FastAPI -> Frontend
```

This is **Option B: realtime from DW**. The STG->DW cron/ETL still normalizes source events and remains operationally required. Do not describe this as final STG-direct cutover.

## Artifact Audit

Before cutover, prove the DDLs are in Git:

```bash
git ls-files sql/clickhouse/streaming | sort
find sql/clickhouse/streaming -maxdepth 1 -type f | sort
```

Both outputs must include:

```text
sql/clickhouse/streaming/040_mart_rt_database.sql
sql/clickhouse/streaming/041_mart_rt_tables.sql
```

`streaming-init-mart-rt.sh` fails if either glob is missing and then verifies these mandatory tables:

```text
dashboard_home_rt, sales_daily_rt, sales_hourly_rt, sales_products_rt,
sales_groups_rt, payments_by_type_rt, cash_overview_rt, fraud_daily_rt,
risk_recent_events_rt, finance_overview_rt, source_freshness,
mart_publication_log
```

## Cutover

```bash
ENV_FILE=/etc/torqmind/prod.env make realtime-cutover
```

The cutover is blocking. It does not activate `USE_REALTIME_MARTS=true` until:

- mart_rt DDL init succeeds;
- Redpanda, Debezium and CDC consumer are running;
- PostgreSQL DW source tables with tenant data have matching raw/current data;
- `torqmind_mart_rt.sales_daily_rt` has tenant data;
- `realtime-validate-cutover.sh` exits zero;
- API facade smoke succeeds with `REALTIME_MARTS_FALLBACK=false`.

## Validation

```bash
ENV_FILE=/etc/torqmind/prod.env make realtime-validate
```

Validation fails on:

- ClickHouse connection errors;
- missing mandatory mart_rt tables;
- empty realtime marts when legacy/source has data;
- divergence above `DECIMAL_TOLERANCE` (default `0.001`);
- API facade failure with `USE_REALTIME_MARTS=true` and `REALTIME_MARTS_FALLBACK=false`.

Compared domains:

- sales daily: rows, `faturamento`, `qtd_vendas`;
- sales hourly: rows, `faturamento`, `qtd_vendas`;
- products/groups: rows and sums;
- payments: total and grouped sum by `category|label`;
- risk/fraud: event counts and impact;
- finance: source current count, total, paid total.

## E2E Smoke

```bash
ENV_FILE=/etc/torqmind/prod.env make realtime-e2e-smoke
```

The smoke inserts a synthetic fixture into `dw.fact_venda` and `dw.fact_venda_item`, waits for `torqmind_raw.cdc_events`, verifies `torqmind_current`, triggers MartBuilder, verifies `torqmind_mart_rt.sales_daily_rt`, and calls the API facade with fallback disabled.

It proves the current DW-origin pipeline only. It does not prove a new STG event bypasses ETL.

## Rollback

```bash
ENV_FILE=/etc/torqmind/prod.env make realtime-rollback
```

Rollback sets `USE_REALTIME_MARTS=false` and recreates the API. Keep `REALTIME_MARTS_FALLBACK=false` for validation; use rollback instead of silent fallback to avoid masking realtime failures.

## Monitoring

```bash
docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" exec -T clickhouse \
  clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
  -q "SELECT * FROM torqmind_mart_rt.source_freshness FINAL ORDER BY domain"
```

```bash
docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" exec -T clickhouse \
  clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
  -q "SELECT table_name, events_total, last_event_at FROM torqmind_ops.cdc_table_state FINAL ORDER BY table_name"
```

## Remaining Risk

The main residual risk is the STG->DW dependency. The migration to STG-direct requires a new streaming transformer that reproduces the canonical ETL semantics for business date, cancelamento, CFOP, payment bridge, dimensions, tenant isolation and reconciliation before Debezium can safely move from `dw.*` to `stg.*`.
