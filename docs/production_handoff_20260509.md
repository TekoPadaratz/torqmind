# Production Handoff 2026-05-09

## Operational State

- Public application responding at `http://redevr.ddns.me:14023`
- `USE_REALTIME_MARTS=true`
- `REALTIME_MARTS_SOURCE=stg`
- `REALTIME_MARTS_FALLBACK=false`
- Debezium connector `torqmind-postgres-cdc` running
- CDC consumer running on analytics host with explicit group `torqmind-cdc-consumer-live`
- Official validator `deploy/scripts/prod-multivm-validate.sh` passes with default invocation
- Proof pack generated with PASS at `tmp/prod-multivm-proof-20260509_011546.json`

## Auth State

- Active platform master: `teko94@gmail.com`
- Active channel admin: `admin@torqmind.com`
- Legacy platform master still present: `admin@torqmind.io`
- Passwords are not recoverable from the database because only hashes are stored
- Official production credentials are sourced from `/etc/torqmind/prod.app.env` via `PLATFORM_MASTER_*` and `CHANNEL_BOOTSTRAP_*`

## Validated Access

- `teko94@gmail.com` login validated against `/api/auth/login`
- `admin@torqmind.com` login validated against `/api/auth/login` with tenant scope for `id_empresa=1`
- Both validated against `/api/auth/me`
- Both validated against `/api/bi/dashboard/home` and `/api/bi/sales/overview`

## Runtime Fixes Included

- Realtime compatibility fallback kept on PostgreSQL when no realtime contract exists
- ETL orchestration now skips `fact_estoque_atual` cleanly when the loader is not installed
- CDC writer sanitizes out-of-range Debezium finance dates before ClickHouse insert
- Multi-VM validator now:
  - auto-detects `CRITICAL_DATA_KEY`
  - checks CDC lag from live Redpanda group lag
  - evaluates freshness only on critical product domains
- Product smoke supports auth-backed token generation and local route override

## Evidence Executed

- `bash -n deploy/scripts/prod-multivm-validate.sh deploy/scripts/realtime-product-screen-smoke.sh`
- `docker compose -f docker-compose.analytics.yml --env-file /etc/torqmind/prod.analytics.env run --rm --no-deps -v "$PWD/apps/cdc_consumer/torqmind_cdc_consumer:/app/torqmind_cdc_consumer:ro" -v "$PWD/apps/cdc_consumer/tests:/app/tests:ro" --entrypoint python cdc-consumer -m unittest tests.test_cdc_consumer.TestClickHouseWriter -v`
- `docker compose -f docker-compose.app.yml --env-file /etc/torqmind/prod.app.env run --rm --no-deps -v "$PWD/apps/api/app:/app/app:ro" --entrypoint python api -m unittest app.test_etl_orchestration app.test_repos_analytics_unit -v`
- `CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-validate.sh --yes`
- `CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-proof.sh`

## Operator Notes

- If a password is suspected invalid, re-seed or rotate it instead of trying to recover it from the database
- For the current production state, the source of truth for official credentials is `/etc/torqmind/prod.app.env`
- The validator now reads `CDC_CONSUMER_GROUP` from analytics env when present