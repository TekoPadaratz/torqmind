# TorqMind — write amplification / SSD wear (ops)

## Sintoma

SSD de dados (PG `.8`) ou analytics (CH `.9`) com wear acelerado; sistema
“pesado”. Causa típica: agent `full_refresh` a cada ~60s + ingest que
reescrevia linhas idênticas → WAL → CDC → merges ClickHouse.

## Mitigações no código (a partir desta versão)

1. Ingest upsert condicional (`payload IS DISTINCT FROM`) — campo `unchanged`
   na resposta.
2. Agent: `full_refresh_min_interval_seconds` default **1800** (30 min).
3. Compose: logs Docker `max-size=50m` / `max-file=3`.
4. Cron ETL: default `OPERATIONAL_INTERVAL_MINUTES=5` (antes 2).

## Medir

```sql
SELECT relname, n_tup_upd, n_tup_ins, n_tup_del
FROM pg_stat_user_tables
WHERE schemaname = 'stg'
ORDER BY n_tup_upd DESC
LIMIT 15;
```

Após deploy API + novo agent: taxa de `n_tup_upd` em `estoque`/`funcionarios`
deve cair drasticamente (só mudanças reais).

Baseline prod (2026-07-27 UTC, pré-deploy desta mitigação):

| relname | n_tup_upd | n_tup_ins |
|---------|-----------|-----------|
| estoque | 41.285.440 | 0 |
| funcionarios | 1.259.995 | 0 |
| planodecontas | 1.120.350 | 0 |

Hygiene semanal: `deploy/scripts/torqmind-host-hygiene.sh`.
