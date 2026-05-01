# TorqMind Realtime Cutover Final

## Status

Implemented as an **STG-direct realtime cutover candidate**.

Final hot path:

```text
Agent/API -> PostgreSQL STG -> Debezium(stg.*) -> Redpanda
  -> CDC Consumer -> ClickHouse torqmind_raw/torqmind_current.stg_*
  -> MartBuilder(source=stg) -> ClickHouse torqmind_mart_rt -> FastAPI
```

DW remains available for audit, reconciliation, rollback and compatibility. It is not the operational realtime BI source when `REALTIME_MARTS_SOURCE=stg`.

Schema compatibility note: the current PostgreSQL model does not have a physical `stg.clientes` table. The API ingest maps the `clientes` dataset into `stg.entidades`; the connector lists `stg.clientes` for future deployments and the publication script includes it only when it exists.

## Versioned DDL Artifacts

Tracked ClickHouse streaming DDL:

```bash
git ls-files sql/clickhouse/streaming | sort
```

Mandatory mart files:

```text
sql/clickhouse/streaming/040_mart_rt_database.sql
sql/clickhouse/streaming/041_mart_rt_tables.sql
```

`020_current_tables.sql` also creates the STG current tables consumed by the MartBuilder, including `stg_comprovantes`, `stg_itenscomprovantes`, `stg_formas_pgto_comprovantes`, dimensions, cash and finance STG state.

## Captured STG Tables

Debezium `table.include.list` now includes:

```text
stg.comprovantes
stg.itenscomprovantes
stg.formas_pgto_comprovantes
stg.turnos
stg.entidades
stg.clientes (optional when present)
stg.produtos
stg.grupoprodutos
stg.funcionarios
stg.usuarios
stg.localvendas
stg.contaspagar
stg.contasreceber
stg.filiais (optional when present)
app.payment_type_map
app.competitor_fuel_prices / app.goals when present
```

`dw.*` remains in the connector for reconciliation and rollback only.

## Guards

`deploy/scripts/prod-realtime-cutover-apply.sh --source stg` is the default. It prepares STG publication, starts streaming, runs `backfill-stg`, waits on raw/current/mart conditions, runs blocking validation, then activates:

```text
USE_REALTIME_MARTS=true
REALTIME_MARTS_SOURCE=stg
REALTIME_MARTS_FALLBACK=false
```

`deploy/scripts/realtime-validate-cutover.sh --source stg` compares canonical STG/PostgreSQL metrics against `torqmind_mart_rt` and exits non-zero on missing tables, empty marts with source data, divergence, ClickHouse failure or API fallback failure.

`deploy/scripts/realtime-e2e-smoke.sh` inserts a synthetic sale into STG canonical tables, waits for Debezium/current, triggers MartBuilder in `source=stg` mode and calls the API facade with fallback disabled. It does not call the STG->DW ETL.

## Backfill

The supported command is:

```bash
# All filiais (production default — no --id-filial):
docker compose -f docker-compose.streaming.yml --env-file "$ENV_FILE" \
  exec -T cdc-consumer python -m torqmind_cdc_consumer.cli backfill-stg \
  --from-date 2025-01-01 --id-empresa 1

# Scoped to specific filial (testing/audit only):
docker compose -f docker-compose.streaming.yml --env-file "$ENV_FILE" \
  exec -T cdc-consumer python -m torqmind_cdc_consumer.cli backfill-stg \
  --from-date 2025-01-01 --id-empresa 1 --id-filial 14458
```

Backfill reads ClickHouse `torqmind_current.stg_*`, which is populated by Debezium initial snapshot/streaming from PostgreSQL STG, and rebuilds `torqmind_mart_rt`. No DW table is required for mart publication in `source=stg` mode.

**Important:** In the orchestrator (`prod-realtime-cutover-apply.sh`), the `--id-filial` flag is for audit/smoke/validation only — it does NOT scope the backfill. Use `--backfill-id-filial` to limit the MartBuilder scope, or omit it for all filiais (production default). `--all-filiais` makes this explicit.

## Rollback

```bash
ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-realtime-cutover-apply.sh --rollback-to-legacy
```

Rollback sets `USE_REALTIME_MARTS=false` and recreates the API container. Use rollback instead of fallback for production acceptance so realtime failures are not masked.

## Residual Risks

- Physical `stg.clientes` is absent; cliente realtime uses `stg.entidades` until a dedicated table is introduced.
- STG JSON date parsing is implemented in ClickHouse best-effort expressions; production acceptance must run `realtime-e2e-smoke.sh` and `realtime-validate-cutover.sh --source stg` against real data.
- DW reconciliation remains necessary during the transition to prove long-window semantic parity, but it is not part of the realtime hot path.

## Timezone Handling

All STG `dt_evento` columns store UTC timestamps. The MartBuilder converts to `America/Sao_Paulo` before computing:
- `data_key` (YYYYMMDD): `formatDateTime(toTimezone(dt_evento, 'America/Sao_Paulo'), '%Y%m%d')`
- `hora` (0-23): `toHour(toTimezone(dt_evento, 'America/Sao_Paulo'))`
- Cash `abertura`/`fechamento`: converted with null-safety

This was fixed to prevent sales occurring at 21:00-23:59 BRT from being attributed to the next UTC day.

The validation script uses `COUNT_TOLERANCE=0.001` for minor TZ boundary drift in count comparisons.
