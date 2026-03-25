# TorqMind Go-Live Runbook

Data: 2026-03-22
Objetivo: validar a base real no PostgreSQL local, promover por dump lógico e religar produção com previsibilidade.

## Pré-requisitos

- Host Ubuntu 24.04 com Docker e Docker Compose.
- Benchmark local já executado sobre a massa real do cliente.
- Arquivo `/etc/torqmind/prod.env` criado a partir de `.env.production.example`.
- Dump lógico gerado com `pg_dump -Fc`.

Variáveis úteis:

```bash
export TM_ROOT=/home/eko/projects/TorqMind
export TM_ENV=/etc/torqmind/prod.env
cd "$TM_ROOT"
```

## T-48h: benchmark local com massa real

Contagens por camada:

```bash
psql -h 127.0.0.1 -p 5432 -U postgres -d TORQMIND -P pager=off -c "
SELECT 'stg.comprovantes' tabela, COUNT(*) total FROM stg.comprovantes
UNION ALL SELECT 'stg.movprodutos', COUNT(*) FROM stg.movprodutos
UNION ALL SELECT 'stg.itensmovprodutos', COUNT(*) FROM stg.itensmovprodutos
UNION ALL SELECT 'stg.formas_pgto_comprovantes', COUNT(*) FROM stg.formas_pgto_comprovantes
UNION ALL SELECT 'dw.fact_comprovante', COUNT(*) FROM dw.fact_comprovante
UNION ALL SELECT 'dw.fact_venda', COUNT(*) FROM dw.fact_venda
UNION ALL SELECT 'dw.fact_venda_item', COUNT(*) FROM dw.fact_venda_item
UNION ALL SELECT 'dw.fact_pagamento_comprovante', COUNT(*) FROM dw.fact_pagamento_comprovante
UNION ALL SELECT 'mart.agg_vendas_diaria', COUNT(*) FROM mart.agg_vendas_diaria
ORDER BY 1;
"
```

Tempos do ETL por etapa:

```bash
psql -h 127.0.0.1 -p 5432 -U postgres -d TORQMIND -P pager=off -c "
SELECT id, step_name, status, rows_processed, ROUND(duration_ms::numeric, 2) AS duration_ms, started_at, finished_at
FROM etl.run_log
ORDER BY id DESC, started_at DESC
LIMIT 100;
"
```

EXPLAIN dos passos quentes:

```bash
psql -h 127.0.0.1 -p 5432 -U postgres -d TORQMIND -P pager=off -c "EXPLAIN (ANALYZE, BUFFERS) SELECT etl.load_fact_comprovante(1, CURRENT_DATE - 30, CURRENT_DATE);"
psql -h 127.0.0.1 -p 5432 -U postgres -d TORQMIND -P pager=off -c "EXPLAIN (ANALYZE, BUFFERS) SELECT etl.load_fact_venda(1, CURRENT_DATE - 30, CURRENT_DATE);"
psql -h 127.0.0.1 -p 5432 -U postgres -d TORQMIND -P pager=off -c "EXPLAIN (ANALYZE, BUFFERS) SELECT etl.load_fact_venda_item(1, CURRENT_DATE - 30, CURRENT_DATE);"
psql -h 127.0.0.1 -p 5432 -U postgres -d TORQMIND -P pager=off -c "EXPLAIN (ANALYZE, BUFFERS) SELECT etl.load_fact_pagamento_comprovante(1, CURRENT_DATE - 30, CURRENT_DATE);"
```

Hot path HTTP:

```bash
curl -s -o /dev/null -w 'home_total=%{time_total}\n' "http://127.0.0.1:8000/bi/dashboard/home?dt_ini=2026-03-01&dt_fim=2026-03-22&id_empresa=1"
curl -s -o /dev/null -w 'sales_total=%{time_total}\n' "http://127.0.0.1:8000/bi/sales/overview?dt_ini=2026-03-01&dt_fim=2026-03-22&id_empresa=1"
curl -s -o /dev/null -w 'cash_total=%{time_total}\n' "http://127.0.0.1:8000/bi/cash/overview?dt_ini=2026-03-01&dt_fim=2026-03-22&id_empresa=1"
curl -s -o /dev/null -w 'fraud_total=%{time_total}\n' "http://127.0.0.1:8000/bi/fraud/overview?dt_ini=2026-03-01&dt_fim=2026-03-22&id_empresa=1"
curl -s -o /dev/null -w 'customers_total=%{time_total}\n' "http://127.0.0.1:8000/bi/customers/overview?dt_ini=2026-03-01&dt_fim=2026-03-22&id_empresa=1"
curl -s -o /dev/null -w 'finance_total=%{time_total}\n' "http://127.0.0.1:8000/bi/finance/overview?dt_ini=2026-03-01&dt_fim=2026-03-22&id_empresa=1"
```

Critérios mínimos antes da promoção:
- incremental sem mudanças abaixo de 30s;
- delta pequeno por tenant operacionalmente viável;
- bootstrap comercial de 365 dias materialmente menor que a janela anterior;
- home e dashboards quentes sem latência anômala.
- cron separado por trilho definido antes do go-live:
  - `operational` com cadência curta e sem `compute_risk_events`
  - `risk` em job independente

## T-24h: congelar a base validada e gerar dump lógico

```bash
pg_dump -h 127.0.0.1 -p 5432 -U postgres -d TORQMIND -Fc -f torqmind_dev_validado_$(date +%Y%m%d_%H%M%S).dump
ls -lh torqmind_dev_validado_*.dump | tail -n1
```

Não promover por cópia de volume, `PGDATA` ou filesystem do cluster.

## T-1h: preparar o servidor Ubuntu

```bash
sudo mkdir -p /etc/torqmind
sudo cp .env.production.example "$TM_ENV"
sudo chmod 600 "$TM_ENV"
```

Preencher no `prod.env` pelo menos:
- `POSTGRES_PASSWORD`
- `API_JWT_SECRET`
- `SEED_PASSWORD`
- `POSTGRES_SHM_SIZE`
- `POSTGRES_SHARED_BUFFERS`
- `DB_POOL_MAX_SIZE`

Subir apenas o Postgres:

```bash
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" up -d postgres
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" ps
```

## T-15min: restaurar e alinhar schema

Recriar banco e restaurar:

```bash
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" exec -T postgres \
  psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS TORQMIND;"
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" exec -T postgres \
  psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE TORQMIND;"
cat torqmind_dev_validado.dump | docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" exec -T postgres \
  pg_restore -U postgres -d TORQMIND -j 4 --clean --if-exists
```

Reaplicar migrations e seed interno:

```bash
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-migrate.sh
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-seed.sh
```

Uso correto do migrator:
- banco novo/vazio: `ENV_FILE="$TM_ENV" ./deploy/scripts/prod-migrate.sh`
- banco já gerenciado por `app.schema_migrations`: mesmo comando; só migrations novas serão aplicadas
- banco existente saudável sem histórico: `ENV_FILE="$TM_ENV" ./deploy/scripts/prod-migrate.sh --baseline-current`

O modo padrão falha de forma segura em banco existente sem histórico para impedir replay da cadeia
legada inteira, incluindo migrations destrutivas como `003_mart_demo.sql`.

Verificar migrations aplicadas:

```bash
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" exec -T postgres \
  psql -U postgres -d TORQMIND -P pager=off -c \
  "SELECT filename, execution_kind, applied_at FROM app.schema_migrations ORDER BY filename;"
```

Subir o restante da stack:

```bash
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-up.sh
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" ps
```

## T-10min: smoke de aplicação

Health:

```bash
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" exec -T api python - <<'PY'
import json, urllib.request
with urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=60) as r:
    print(json.dumps(json.loads(r.read().decode()), indent=2, ensure_ascii=False))
PY
```

Endpoints críticos:

```bash
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" exec -T api python - <<'PY'
import json, urllib.request
BASE='http://127.0.0.1:8000'

def req(path, method='GET', data=None, token=None):
    headers={'Content-Type':'application/json'}
    if token:
        headers['Authorization']=f'Bearer {token}'
    body=None if data is None else json.dumps(data).encode()
    request=urllib.request.Request(BASE + path, method=method, headers=headers, data=body)
    with urllib.request.urlopen(request, timeout=180) as response:
        return response.status, json.loads(response.read().decode())

_, login = req('/auth/login', 'POST', {'email': 'owner@empresa1.com', 'password': 'TorqMind@123'})
token = login['access_token']
checks = {
    'auth_me': req('/auth/me', token=token)[0],
    'home': req('/bi/dashboard/home?dt_ini=2026-03-01&dt_fim=2026-03-22&id_empresa=1', token=token)[0],
    'sales': req('/bi/sales/overview?dt_ini=2026-03-01&dt_fim=2026-03-22&id_empresa=1', token=token)[0],
    'cash': req('/bi/cash/overview?dt_ini=2026-03-01&dt_fim=2026-03-22&id_empresa=1', token=token)[0],
    'fraud': req('/bi/fraud/overview?dt_ini=2026-03-01&dt_fim=2026-03-22&id_empresa=1', token=token)[0],
    'customers': req('/bi/customers/overview?dt_ini=2026-03-01&dt_fim=2026-03-22&id_empresa=1', token=token)[0],
    'finance': req('/bi/finance/overview?dt_ini=2026-03-01&dt_fim=2026-03-22&id_empresa=1', token=token)[0],
    'notifications': req('/bi/notifications?id_empresa=1&limit=10', token=token)[0],
}
print(json.dumps(checks, indent=2))
PY
```

Critério: todos os status `200`.

Critério funcional adicional:
- `home` deve retornar metadata de cobertura por bloco, sem zerar fraude/churn/financeiro silenciosamente;
- `cash` deve trazer `historical` e `live_now`;
- `auth_me` deve apontar `home_path` para `/dashboard`, não para `/scope`.

## T+1h: validação operacional

Rodar os trilhos manualmente:

```bash
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-etl-operational.sh
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-etl-risk.sh
```

Checagens obrigatórias:
- o log do trilho `operational` precisa trazer `risk_events_skipped=true` e `risk_events_skip_reason=track_excludes_risk`;
- o trilho `risk` precisa trazer `refresh_domains.risk=true` quando houver eventos recalculados;
- se um trilho já estiver segurando o tenant, o outro deve sair com `skipped=true` e `reason=tenant_busy` quando rodado com `--skip-busy-tenants`.

Contagens e eventos recentes:

```bash
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" exec -T postgres psql -U postgres -d TORQMIND -P pager=off -c "
SELECT 'stg.comprovantes' AS tabela, COUNT(*) AS total FROM stg.comprovantes WHERE id_empresa=1
UNION ALL SELECT 'stg.movprodutos', COUNT(*) FROM stg.movprodutos WHERE id_empresa=1
UNION ALL SELECT 'stg.itensmovprodutos', COUNT(*) FROM stg.itensmovprodutos WHERE id_empresa=1
UNION ALL SELECT 'dw.fact_comprovante', COUNT(*) FROM dw.fact_comprovante WHERE id_empresa=1
UNION ALL SELECT 'dw.fact_venda', COUNT(*) FROM dw.fact_venda WHERE id_empresa=1
UNION ALL SELECT 'dw.fact_venda_item', COUNT(*) FROM dw.fact_venda_item WHERE id_empresa=1
UNION ALL SELECT 'dw.fact_pagamento_comprovante', COUNT(*) FROM dw.fact_pagamento_comprovante WHERE id_empresa=1
UNION ALL SELECT 'mart.agg_vendas_diaria', COUNT(*) FROM mart.agg_vendas_diaria WHERE id_empresa=1
UNION ALL SELECT 'app.notifications', COUNT(*) FROM app.notifications WHERE id_empresa=1
ORDER BY tabela;
"
```

```bash
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" exec -T postgres psql -U postgres -d TORQMIND -P pager=off -c "
SELECT id, step_name, status, rows_processed, ROUND(duration_ms::numeric, 2) AS duration_ms
FROM etl.run_log
ORDER BY id DESC, started_at DESC
LIMIT 30;
"
```

Cron recomendado após o smoke:
- baseline segura:
  - `*/5 * * * *` -> `prod-etl-operational.sh`
  - `*/10 * * * *` -> `prod-etl-risk.sh`
- evidência local usada para a decisão em `2026-03-25`:
  - `operational` em tenant 1: `122.64s`
  - `risk` em tenant 1 após delta operacional: `87.84s`
- decisão: não reativar cron de 1 minuto ainda. Ele ficou lock-safe, mas não sustentou cadência limpa nessa massa.

Habilitar agent e cron somente depois dessa validação.

## Rotina diária

```bash
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-etl-operational.sh
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-etl-risk.sh
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-purge-sales-history.sh
ENV_FILE="$TM_ENV" ./deploy/scripts/platform-billing-daily.sh
```

Regras:
- não misturar o purge diário com o cron do ETL operacional;
- manter `prod-etl-incremental.sh` só para compatibilidade/manual ou fallback controlado;
- manter o agent desligado durante restore e migração;
- religar agent e cron só após smoke e contagens fecharem;
- nunca promover por cópia física do cluster.

## Rollback

1. Parar agent e cron.
2. Reverter containers para a tag estável anterior.
3. Se necessário, restaurar o dump lógico anterior:

```bash
cat backup_pre_release.dump | docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" exec -T postgres \
  pg_restore -U postgres -d TORQMIND -j 4 --clean --if-exists
```
