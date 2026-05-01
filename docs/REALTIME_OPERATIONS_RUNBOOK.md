# TorqMind Realtime Operations Runbook

## Quick Reference

| Command | What it does |
|---------|--------------|
| `make streaming-init-mart-rt` | Applies tracked `040/041` mart_rt DDL and verifies mandatory tables |
| `make realtime-cutover` | Guarded STG-direct cutover (`--source stg`) |
| `make realtime-validate` | Blocking STG vs mart_rt/API validation with fallback disabled |
| `make realtime-backfill` | Rebuilds mart_rt from `torqmind_current.stg_*` |
| `make realtime-e2e-smoke` | Inserts an STG fixture and verifies raw/current/MartBuilder/API |
| `make realtime-rollback` | Sets `USE_REALTIME_MARTS=false` and restarts API |
| `make streaming-status` | Shows Debezium/Redpanda/CDC status and STG topics |

## Architecture

```text
Agent/API -> PostgreSQL STG -> Debezium(stg.*)
  -> Redpanda -> CDC Consumer -> ClickHouse torqmind_raw/torqmind_current.stg_*
  -> MartBuilder(source=stg) -> ClickHouse torqmind_mart_rt -> FastAPI -> Frontend
```

DW PostgreSQL remains for audit, reconciliation, compatibility and emergency rollback. It is not the realtime BI engine when `REALTIME_MARTS_SOURCE=stg`.

Clientes compatibility: this repo currently stores `clientes` ingestion in `stg.entidades`. `stg.clientes` is optional in the connector/publication and will be included when a physical table exists.

## Artifact Audit

```bash
git ls-files sql/clickhouse/streaming | sort
find sql/clickhouse/streaming -maxdepth 1 -type f | sort
```

Both outputs must include:

```text
sql/clickhouse/streaming/040_mart_rt_database.sql
sql/clickhouse/streaming/041_mart_rt_tables.sql
```

`020_current_tables.sql` must include STG current tables such as `stg_comprovantes`, `stg_itenscomprovantes` and `stg_formas_pgto_comprovantes`.

## Cutover

```bash
# Full cutover (all filiais — production default)
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-realtime-cutover-apply.sh \
  --yes --with-backfill --source stg --from-date 2025-01-01 --id-empresa 1 --all-filiais

# Scoped cutover (specific filial — testing/homologation only)
# ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-realtime-cutover-apply.sh \
#   --yes --with-backfill --source stg --from-date 2025-01-01 --id-empresa 1 \
#   --id-filial 14458 --backfill-id-filial 14458
#
# NOTE: --id-filial = audit/smoke scope. --backfill-id-filial = MartBuilder scope.
# Without --backfill-id-filial, backfill covers ALL filiais.
```

The cutover is blocking and only activates realtime after:

- ClickHouse raw/current/ops/mart_rt DDL init succeeds.
- PostgreSQL publication includes required STG tables.
- Redpanda, Debezium and CDC consumer are running.
- PostgreSQL STG source tables with tenant data have matching raw/current STG data.
- `backfill-stg` publishes rows into `torqmind_mart_rt`.
- `realtime-validate-cutover.sh --source stg` exits zero.
- API facade smoke succeeds with `USE_REALTIME_MARTS=true`, `REALTIME_MARTS_SOURCE=stg` and `REALTIME_MARTS_FALLBACK=false`.

## Validation

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/realtime-validate-cutover.sh --source stg
```

Validation fails on:

- ClickHouse connection errors.
- Missing mandatory `torqmind_mart_rt` tables.
- Empty realtime marts while STG has data.
- Divergence above `DECIMAL_TOLERANCE` (default `0.001`).
- API facade failure with fallback disabled.
- Effective API source different from `stg`.

Compared domains include canonical STG sales totals/counts, item totals, payments, cancellations/risk and finance counts/sums.

## E2E Smoke

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/realtime-e2e-smoke.sh
```

The smoke inserts into:

```text
stg.comprovantes
stg.itenscomprovantes
stg.formas_pgto_comprovantes
```

Then it waits for raw/current STG CDC, triggers MartBuilder with `source=stg`, verifies `sales_daily_rt`, and calls the API facade with fallback disabled. It does not run STG->DW ETL.

## Rollback

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-realtime-cutover-apply.sh --rollback-to-legacy
```

Rollback sets `USE_REALTIME_MARTS=false` and recreates API. Do not enable fallback as an acceptance mechanism; fallback is emergency behavior, not proof.

## Monitoring

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/streaming-status.sh
```

```bash
docker compose -f docker-compose.prod.yml --env-file "$ENV_FILE" exec -T clickhouse \
  clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
  -q "SELECT * FROM torqmind_mart_rt.source_freshness FINAL ORDER BY domain"
```

## Timezone Model

All timestamps in STG (`dt_evento`) are stored in **UTC** (both PostgreSQL and ClickHouse).
Business logic (day boundaries, hourly aggregations, data_key extraction) uses `America/Sao_Paulo`.

The MartBuilder applies `toTimezone(dt_evento, 'America/Sao_Paulo')` before:
- Extracting `data_key` (YYYYMMDD in BRT)
- Computing `hora` for hourly aggregations
- Deriving `abertura`/`fechamento` in cash overview

This ensures a sale at 23:30 BRT (02:30 UTC next day) is correctly attributed to the BRT calendar day.

## Data Profile

```bash
ENV_FILE=.env.e2e.local COMPOSE_FILE=docker-compose.prod.yml \
  bash deploy/scripts/realtime-sales-data-profile.sh --id-empresa 1
```

Shows: date range, daily volume, hourly distribution, filial breakdown, payment types, cash/fraud/finance summaries.

## Remaining Risk

The STG-direct path is implemented in code and scripts, but production acceptance still requires running the E2E smoke and blocking validation against the target environment. If either cannot run, the release is not operationally concluded.
