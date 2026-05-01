# TorqMind Realtime Cutover Artifacts

## Status

Implemented as a **DW-origin realtime cutover candidate**. This is not the final STG-direct architecture.

Current path:

```text
Agent/API -> PostgreSQL STG -> ETL STG->DW -> Debezium(dw.*) -> Redpanda
  -> CDC Consumer -> ClickHouse torqmind_raw/torqmind_current
  -> MartBuilder -> ClickHouse torqmind_mart_rt -> FastAPI repos_analytics
```

Desired final path:

```text
Agent/API -> PostgreSQL STG -> Debezium(stg.*) -> Redpanda
  -> CDC Consumer STG transformer -> ClickHouse current/mart_rt -> FastAPI
```

The ETL STG->DW cron remains an operational dependency in the current implementation. If it stops, fresh STG events do not reach Debezium because Debezium is subscribed to `dw.*`, not `stg.*`.

## Versioned DDL Artifacts

The realtime mart DDL lives in tracked ClickHouse streaming SQL:

```bash
git ls-files sql/clickhouse/streaming | sort
```

Required files:

```text
sql/clickhouse/streaming/040_mart_rt_database.sql
sql/clickhouse/streaming/041_mart_rt_tables.sql
```

`040_mart_rt_database.sql` creates `torqmind_mart_rt` and `mart_publication_log`.
`041_mart_rt_tables.sql` creates the dashboard/domain tables.

Mandatory runtime tables:

```text
torqmind_mart_rt.dashboard_home_rt
torqmind_mart_rt.sales_daily_rt
torqmind_mart_rt.sales_hourly_rt
torqmind_mart_rt.sales_products_rt
torqmind_mart_rt.sales_groups_rt
torqmind_mart_rt.payments_by_type_rt
torqmind_mart_rt.cash_overview_rt
torqmind_mart_rt.fraud_daily_rt
torqmind_mart_rt.risk_recent_events_rt
torqmind_mart_rt.finance_overview_rt
torqmind_mart_rt.source_freshness
torqmind_mart_rt.mart_publication_log
```

`customers_churn_rt` may also exist as an extra mart table, but it is not part of the blocking cutover contract yet.

## Blocking Guards

`deploy/scripts/streaming-init-mart-rt.sh`:

- requires `ENV_FILE`;
- uses `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD`;
- never prints the password;
- fails if no `040_*.sql` or no `041_*.sql` exists;
- verifies all 12 mandatory `torqmind_mart_rt` tables after applying DDL.

`deploy/scripts/realtime-validate-cutover.sh`:

- fails on ClickHouse connection errors;
- fails on missing mandatory mart_rt tables;
- fails when realtime marts are empty while source/legacy has data;
- compares sales daily/hourly, products, groups, payments by label, risk/fraud, and finance;
- executes the API facade with `USE_REALTIME_MARTS=true` and `REALTIME_MARTS_FALLBACK=false`.

`deploy/scripts/prod-realtime-cutover-apply.sh` only activates `USE_REALTIME_MARTS=true` after:

- mart_rt DDL init passes;
- Redpanda, Debezium and CDC consumer are running;
- raw/current contain data for source DW tables that have tenant data;
- mart_rt has data;
- blocking validation returns zero;
- API smoke passes with fallback disabled.

## Rollback

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-realtime-cutover-apply.sh --rollback-to-legacy
```

Rollback sets `USE_REALTIME_MARTS=false` and recreates the API container. Data remains in ClickHouse and can be inspected/backfilled again.

## STG-Direct Migration Plan

To make this a true final STG-direct cutover:

1. Register Debezium for `stg.comprovantes`, `stg.itenscomprovantes`, `stg.formas_pgto_comprovantes`, `stg.turnos`, `stg.financeiro`, and required cadastro STG tables.
2. Add `torqmind_current` STG-shaped current tables or a transformer that emits the existing current facts from STG events.
3. Port the canonical ETL semantics into deterministic streaming transforms: business date, cancelamento, CFOP commercial filter, payment bridge, dimensions, and tenant/filial boundaries.
4. Dual-run STG-direct current vs DW-origin current and compare row counts, sales totals, payments, risk events, and finance balances.
5. Switch Debezium table include list from `dw.*` to `stg.*` only after parity is proven and documented.

Until those steps are complete, this release is a DW-origin realtime cutover candidate, not the final STG-direct architecture.
