# TorqMind Production Multi-VM Runbook

This runbook deploys TorqMind on three clean Ubuntu servers:

- PostgreSQL server: canonical transactional PostgreSQL with STG, DW, app/auth/config and logical replication.
- Analytics server: ClickHouse, Redpanda, Debezium Connect, CDC Consumer and MartBuilder.
- App server: FastAPI, Next.js, Nginx, orchestration scripts and the 2-minute incremental ETL cron.

The App server is the orchestrator. It reaches the other servers over SSH and all private service traffic must stay on the private network.

## Files

Compose files:

- `docker-compose.pg.yml`: PostgreSQL only.
- `docker-compose.analytics.yml`: ClickHouse, Redpanda, Debezium and CDC Consumer.
- `docker-compose.app.yml`: API, Web and Nginx only.

Environment examples:

- `deploy/env/cluster.env.example` -> `/etc/torqmind/cluster.env` on the orchestrator.
- `deploy/env/prod.pg.env.example` -> `/etc/torqmind/prod.pg.env` on the PostgreSQL server.
- `deploy/env/prod.analytics.env.example` -> `/etc/torqmind/prod.analytics.env` on the Analytics server.
- `deploy/env/prod.app.env.example` -> `/etc/torqmind/prod.app.env` on the App server.

Never commit real credentials. Keep real env files under `/etc/torqmind` with mode `600`.

## Network And Firewall

PostgreSQL server:

- Allow TCP `5432` only from the App private IP and Analytics private IP.
- Allow SSH only from the admin/orchestrator network.
- Deny public access.

Analytics server:

- Allow ClickHouse HTTP `8123` and native `9000` only from the App private IP.
- Debezium REST is bound to `127.0.0.1:18083` by default.
- Redpanda is internal to Docker by default.
- Allow SSH only from the admin/orchestrator network.

App server:

- Allow public `80` and, after TLS is configured, `443`.
- Allow SSH from the admin network.
- Keep API and Web internal behind Nginx; do not publish `8000` or `3000`.

Optional host preparation with UFW:

```bash
sudo TORQMIND_SSH_USER=deploy TORQMIND_APP_PRIVATE_IP=<app-private-ip> TORQMIND_ANALYTICS_PRIVATE_IP=<analytics-private-ip> \
  ./deploy/scripts/prod-multivm-prepare-host.sh --role pg --yes --with-ufw

sudo TORQMIND_SSH_USER=deploy TORQMIND_APP_PRIVATE_IP=<app-private-ip> \
  ./deploy/scripts/prod-multivm-prepare-host.sh --role analytics --yes --with-ufw

sudo TORQMIND_SSH_USER=deploy \
  ./deploy/scripts/prod-multivm-prepare-host.sh --role app --yes --with-ufw
```

## Environment Setup

On the App/orchestrator server:

```bash
sudo mkdir -p /etc/torqmind
sudo cp deploy/env/cluster.env.example /etc/torqmind/cluster.env
sudo chmod 600 /etc/torqmind/cluster.env
```

Edit `/etc/torqmind/cluster.env`:

```bash
TORQMIND_SSH_USER=deploy
TORQMIND_PG_HOST=<pg-private-or-ssh-host>
TORQMIND_ANALYTICS_HOST=<analytics-private-or-ssh-host>
TORQMIND_APP_HOST=<app-private-or-ssh-host>
TORQMIND_PG_PRIVATE_IP=<pg-private-ip>
TORQMIND_ANALYTICS_PRIVATE_IP=<analytics-private-ip>
TORQMIND_APP_PRIVATE_IP=<app-private-ip>
TORQMIND_REPO_DIR=/home/deploy/apps/torqmind
TORQMIND_BRANCH=nova-branch-limpa
TORQMIND_ENV_DIR=/etc/torqmind
TORQMIND_REPO_URL=<git-url>
```

On each server, create the role-specific env from the example and replace placeholders:

```bash
sudo mkdir -p /etc/torqmind
sudo cp deploy/env/prod.pg.env.example /etc/torqmind/prod.pg.env
sudo cp deploy/env/prod.analytics.env.example /etc/torqmind/prod.analytics.env
sudo cp deploy/env/prod.app.env.example /etc/torqmind/prod.app.env
sudo chmod 600 /etc/torqmind/*.env
```

Only copy the file needed by that server. In production:

- `PG_HOST` in `prod.app.env` must be the PostgreSQL private IP or private DNS.
- `CLICKHOUSE_HOST` in `prod.app.env` must be the Analytics private IP or private DNS.
- `DATABASE_URL` must use the same remote PostgreSQL host.
- `REALTIME_MARTS_FALLBACK=false` is required for production proof.

## One-Command Bootstrap

Recommended from the App/orchestrator server:

```bash
ENV_FILE=/etc/torqmind/prod.app.env \
CLUSTER_ENV=/etc/torqmind/cluster.env \
./deploy/scripts/prod-multivm-bootstrap.sh --yes --with-ddl --with-cron --validate
```

The bootstrap does:

1. Validates SSH for the three hosts.
2. Validates Docker and Compose.
3. Validates remote env files.
4. Syncs the configured git branch on every host.
5. Starts PostgreSQL with logical replication settings.
6. Builds the API image on App and runs migrations against remote PostgreSQL.
7. Runs production seed/auth in `master-only` mode.
8. Creates Debezium heartbeat, signal table and publication on PostgreSQL.
9. Starts Analytics services.
10. Applies ClickHouse raw/current/ops/mart_rt DDL.
11. Registers Debezium against the remote PostgreSQL host.
12. Starts API, Web and Nginx.
13. Installs the 2-minute incremental ETL cron using `flock`.
14. Runs blocking validation.
15. Generates JSON proof.

Use `--dry-run` first to inspect SSH and compose commands without changing servers.

## Exact Production Sequence

Use this order on clean Ubuntu servers:

1. Fill `/etc/torqmind/cluster.env` on the App/orchestrator server with the SSH hosts, private IPs, repo URL, repo dir and branch.
2. Fill only the role-specific env on each server:
   - PostgreSQL: `/etc/torqmind/prod.pg.env`
   - Analytics: `/etc/torqmind/prod.analytics.env`
   - App: `/etc/torqmind/prod.app.env`
3. Prepare SSH keys so the App/orchestrator user can run `ssh -o BatchMode=yes deploy@<host> true` for all three hosts.
4. Run `prod-multivm-prepare-host.sh` on each server, optionally with `--with-ufw`.
5. From the App/orchestrator server, run `prod-multivm-sync-code.sh --yes`.
6. Run the bootstrap:

```bash
ENV_FILE=/etc/torqmind/prod.app.env \
CLUSTER_ENV=/etc/torqmind/cluster.env \
./deploy/scripts/prod-multivm-bootstrap.sh --yes --with-ddl --with-cron --validate
```

7. Run blocking validation and proof again if any data/bootstrap step was repeated.
8. Only after infrastructure validation passes, point the Agent to the public API/Nginx URL on the App server.
9. Run the Agent historical load and validate PostgreSQL STG counts.
10. Run the first full/incremental STG->DW load from the App server.
11. Bootstrap ClickHouse from PostgreSQL STG and rebuild `mart_rt`.
12. Register/validate Debezium and keep CDC Consumer/MartBuilder active.
13. Enable realtime only after proof PASS by setting `USE_REALTIME_MARTS=true`, `REALTIME_MARTS_SOURCE=stg` and `REALTIME_MARTS_FALLBACK=false`.
14. Generate final proof JSON and keep it with the deployment evidence.

## Data Bootstrap Sequence

For real customer data, the Agent sends history to the public API on the App server. ClickHouse does not populate itself.

1. Bootstrap empty infrastructure and run migrations.
2. Start API.
3. Configure the Agent to send full history to the App public API.
4. Validate PostgreSQL STG received the history.
5. Run the first STG->DW load:

```bash
CLUSTER_ENV=/etc/torqmind/cluster.env \
ssh deploy@<app-host> 'cd /home/deploy/apps/torqmind && ENV_FILE=/etc/torqmind/prod.app.env COMPOSE_FILE=docker-compose.app.yml TRACK=full ./deploy/scripts/prod-etl-incremental.sh'
```

6. Bootstrap PostgreSQL STG into ClickHouse current on Analytics:

```bash
ssh deploy@<analytics-host> 'cd /home/deploy/apps/torqmind && ENV_FILE=/etc/torqmind/prod.analytics.env COMPOSE_FILE=docker-compose.analytics.yml ./deploy/scripts/realtime-bootstrap-stg.sh --id-empresa 1 --from-date 2025-01-01'
```

7. Rebuild `mart_rt` from slim:

```bash
ssh deploy@<analytics-host> 'cd /home/deploy/apps/torqmind && ENV_FILE=/etc/torqmind/prod.analytics.env COMPOSE_FILE=docker-compose.analytics.yml STREAMING_COMPOSE_FILE=docker-compose.analytics.yml ./deploy/scripts/realtime-rebuild-mart-rt-from-slim.sh --yes --drop-recreate --id-empresa 1 --from-date 2025-01-01'
```

8. Register and validate Debezium.
9. Keep CDC Consumer and MartBuilder active.
10. Keep the App cron active every 2 minutes.
11. Enable realtime only after validation PASS:

```bash
USE_REALTIME_MARTS=true
REALTIME_MARTS_SOURCE=stg
REALTIME_MARTS_FALLBACK=false
```

Restart App services after changing these flags:

```bash
CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-up.sh --yes
```

## Validation And Proof

Blocking validation:

```bash
CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-validate.sh --yes
```

Proof JSON:

```bash
CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-proof.sh --output /home/deploy/logs/torqmind-proof.json
```

The validator fails if:

- PostgreSQL is unreachable or not configured with `wal_level=logical`.
- Required schemas are missing.
- ClickHouse, Redpanda, Debezium or CDC Consumer are not healthy.
- Debezium connector or tasks are not `RUNNING`.
- CDC lag exceeds threshold.
- `REALTIME_MARTS_FALLBACK=true`.
- API effective config points to local `postgres` or `clickhouse`.
- `data_key=0` exists in realtime sales marts.
- The critical data key, default `20260430`, is missing.
- Product screen smoke fails.

## Cron

Install or repair the App incremental ETL cron:

```bash
CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-install-cron.sh --yes
```

It writes:

- Log: `/home/deploy/logs/prod-etl-incremental-cron.log`
- Lock: `/tmp/torqmind-prod-etl-incremental-cron.lock`
- Schedule: every 2 minutes by default

The installer preserves existing crontab lines and replaces only the TorqMind multi-VM ETL entry.

## Local And CI Tests

Default API tests must not require Docker service DNS such as `postgres`. DB-backed API suites are marked `integration_db` and skipped unless explicitly enabled.

Unit/local API suite:

```bash
pytest apps/api -q
```

PostgreSQL integration suite inside Docker Compose:

```bash
ENV_FILE=.env \
COMPOSE_FILE=docker-compose.yml \
RUN_INTEGRATION=true \
./deploy/scripts/test-api-in-docker.sh
```

The script starts `postgres` and `api`, rebuilds the API image when needed, runs migrations, installs `pytest` inside the API container if needed, then runs:

```bash
python -m pytest . -q --run-db-integration
```

By default `RUN_INTEGRATION=false`, so the same script runs only the non-DB API tests inside the container. For a narrow DB-backed smoke, pass a pytest target:

```bash
ENV_FILE=.env \
COMPOSE_FILE=docker-compose.yml \
RUN_INTEGRATION=true \
./deploy/scripts/test-api-in-docker.sh app/test_ingest_time_parsing.py
```

## Status

```bash
CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-status.sh
```

## Rollback

Fast serving rollback:

1. On App env, set:

```bash
USE_REALTIME_MARTS=false
REALTIME_MARTS_FALLBACK=false
```

2. Restart App services:

```bash
CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-up.sh --yes
```

Data rollback rules:

- Do not delete canonical PostgreSQL data.
- Do not run `docker system prune --volumes`.
- If ClickHouse marts are wrong, rebuild ClickHouse from PostgreSQL STG/current; do not modify STG to match ClickHouse.

## Backups

PostgreSQL:

```bash
pg_dump -Fc -h <pg-private-ip> -U <user> -d TORQMIND -f torqmind-$(date +%Y%m%d_%H%M%S).dump
```

ClickHouse:

- Prefer filesystem or object-storage backup for `/var/lib/clickhouse` snapshots after quiescing writes, or use the ClickHouse backup tool adopted by the infrastructure.
- ClickHouse is derivable from PostgreSQL STG plus CDC, but backup reduces recovery time.

Configuration:

- Back up `/etc/torqmind/*.env` out of band in a secrets manager or encrypted vault.

## Troubleshooting

SSH fails:

- Check `TORQMIND_SSH_USER`, hostnames and authorized keys.
- Run `ssh -o BatchMode=yes deploy@<host> true`.

App points to local services:

- Fix `/etc/torqmind/prod.app.env`.
- `PG_HOST` cannot be `postgres`, `localhost` or `127.0.0.1`.
- `CLICKHOUSE_HOST` cannot be `clickhouse`, `localhost` or `127.0.0.1`.

Debezium is not running:

```bash
ssh deploy@<analytics-host> 'curl -fsS http://127.0.0.1:18083/connectors/torqmind-postgres-cdc/status | jq'
```

CDC lag or freshness is stale:

- Check Redpanda health.
- Check CDC Consumer logs.
- Check ClickHouse memory errors.
- Re-run MartBuilder/backfill only after raw/current are healthy.

Marts are empty:

- Confirm Agent sent data to API.
- Confirm PostgreSQL STG counts.
- Run STG bootstrap to ClickHouse.
- Rebuild `mart_rt` from slim.
- Re-run validator and proof.
