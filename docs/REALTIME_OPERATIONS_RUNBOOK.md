# TorqMind Realtime Operations Runbook

## Quick Reference

| Command | What it does |
|---------|--------------|
| `make realtime-cutover` | Full cutover (builds, migrates, backfills, validates, activates) |
| `make realtime-validate` | Compare legacy vs mart_rt (bloqueante) |
| `make realtime-backfill` | Rebuild mart_rt from current data |
| `make realtime-rollback` | Disable realtime, revert to legacy batch marts |
| `make realtime-e2e-smoke` | Insert test sale → verify full pipeline |
| `make streaming-status` | Show Debezium/Redpanda/CDC consumer status |
| `make streaming-init-mart-rt` | Apply/re-apply mart_rt DDLs |

## Architecture (Production)

```
Agent → PostgreSQL STG → (cron 2min) → PostgreSQL DW
                                              │
                                     Debezium CDC (<1s)
                                              │
                                          Redpanda
                                              │
                                     CDC Consumer
                                              │
                              ┌───────────────┴───────────────┐
                              ▼                               ▼
                     ClickHouse raw/current           MartBuilder (<1s)
                                                              │
                                                              ▼
                                                    ClickHouse mart_rt
                                                              │
                                                              ▼
                                                     FastAPI → Frontend
```

Total latency: ~2 minutes (dominated by STG→DW cron).

## Monitoring

### Source Freshness
```bash
# Via API (platform admin)
curl -H "Authorization: Bearer $TOKEN" http://localhost:18000/platform/streaming-health

# Direct ClickHouse
docker compose -f docker-compose.prod.yml exec clickhouse clickhouse-client \
  --user torqmind --password $CH_PASS \
  -q "SELECT * FROM torqmind_mart_rt.source_freshness FINAL ORDER BY domain"
```

### CDC Lag
```bash
docker compose -f docker-compose.streaming.yml exec clickhouse clickhouse-client \
  --user torqmind --password $CH_PASS \
  -q "SELECT * FROM torqmind_ops.cdc_table_state FINAL ORDER BY table_name"
```

### Debezium Status
```bash
docker compose -f docker-compose.streaming.yml exec debezium-connect \
  curl -s http://localhost:8083/connectors/torqmind-postgres-cdc/status | jq
```

## Troubleshooting

### mart_rt has stale data (lag > 5 minutes)
1. Check Debezium connector status
2. Check CDC consumer logs: `docker compose -f docker-compose.streaming.yml logs cdc-consumer --tail=50`
3. Check if Redpanda has messages: `make streaming-status`
4. If connector is FAILED, restart: `make streaming-register-debezium`

### Validation shows DIVERGENT
1. Run `make realtime-validate` to see which metrics diverge
2. Check if legacy mart was refreshed more recently
3. Run backfill for affected period: `make realtime-backfill`
4. Re-validate

### API returns legacy data despite USE_REALTIME_MARTS=true
1. Check `REALTIME_MARTS_FALLBACK=true` — if set, failures silently fall back
2. Check API logs for "Realtime mart read failed" warnings
3. Verify mart_rt has data: query ClickHouse directly
4. Set `REALTIME_MARTS_FALLBACK=false` to force errors instead of silent fallback

### Rollback to Legacy
```bash
make realtime-rollback
```
This sets USE_REALTIME_MARTS=false and restarts the API. Instant. No data loss.

## Feature Flags

| Flag | Default | Description |
|------|---------|-------------|
| `USE_REALTIME_MARTS` | false | Master switch for realtime path |
| `REALTIME_MARTS_DOMAINS` | all | Comma-separated domains to serve from mart_rt |
| `REALTIME_MARTS_FALLBACK` | true | Fall back to legacy on realtime errors |
| `ENABLE_MART_BUILDER` | true | CDC consumer builds marts after each flush |

## Backfill

Full backfill from current data (all history):
```bash
docker compose -f docker-compose.streaming.yml exec cdc-consumer \
  python -m torqmind_cdc_consumer.cli backfill --from-date 2025-01-01 --id-empresa 1
```

Backfill specific filial:
```bash
docker compose -f docker-compose.streaming.yml exec cdc-consumer \
  python -m torqmind_cdc_consumer.cli backfill --from-date 2025-04-01 --id-empresa 1 --id-filial 2
```

Check backfill status:
```bash
docker compose -f docker-compose.streaming.yml exec cdc-consumer \
  python -m torqmind_cdc_consumer.cli status
```
