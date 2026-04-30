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

## Apply unico recomendado

Para homologacao controlada, prefira o orquestrador unico em vez de disparar comandos soltos.

Apply padrao com ClickHouse completo:

```bash
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-homologation-apply.sh --yes --full-clickhouse --id-empresa 1 --id-filial 14458
```

Apply com streaming 2.0 em paralelo:

```bash
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-homologation-apply.sh --yes --full-clickhouse --with-streaming --id-empresa 1 --id-filial 14458
```

Apply com rebuild derivado desde a STG antes do ClickHouse full:

```bash
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-homologation-apply.sh --yes --rebuild-dw-from-stg --from-date 2025-01-01 --id-empresa 1 --id-filial 14458
```

Apply apenas para reconstruir o DW PostgreSQL, sem republicar ClickHouse:

```bash
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-homologation-apply.sh --yes --rebuild-dw-from-stg --allow-dw-only --skip-clickhouse --from-date 2025-01-01 --id-empresa 1
```

Apply incremental sem streaming:

```bash
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-homologation-apply.sh --yes --no-streaming
```

Logs:

- apply principal em `/home/deploy/logs/torqmind-homologation-apply-YYYYMMDD_HHMMSS.log`
- pipeline recorrente em `/home/deploy/logs/torqmind-etl-pipeline.log`

Detalhes completos de flags, dry-run e rollback basico: `docs/HOMOLOGATION_APPLY_RUNBOOK.md`.
Runbook do rebuild derivado puro: `docs/DERIVED_REBUILD_FROM_STG_RUNBOOK.md`.
Use `--allow-dw-only` apenas para verificacao intermediaria do PostgreSQL DW; para religar o serving analitico da API, o fluxo recomendado continua sendo republicar ClickHouse no mesmo apply.

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
psql -h 127.0.0.1 -p 5432 -U postgres -d TORQMIND -P pager=off -c "EXPLAIN (ANALYZE, BUFFERS) SELECT etl.load_fact_pagamento_comprovante(1);"
psql -h 127.0.0.1 -p 5432 -U postgres -d TORQMIND -P pager=off -c "EXPLAIN (ANALYZE, BUFFERS) SELECT etl.load_fact_pagamento_comprovante_detail(1);"
```

Sinais obrigatórios após o refactor do eixo comprovante/pagamento:

- `etl.pagamento_comprovante_bridge` deve existir e ser atualizada por `etl.load_fact_comprovante`.
- o `meta` do step `fact_pagamento_comprovante` em `etl.run_log` deve expor:
  - `candidate_count`
  - `bridge_miss_count`
  - `bridge_resolve_ms`
  - `upsert_inserts`
  - `upsert_updates`
  - `conflict_count`
  - `notification_rows`
  - `notification_ms`
  - `total_ms`
- `mart.pagamentos_anomalias_diaria` deve manter `insight_id` textual e `insight_id_hash` numérico.

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
sudo systemctl enable --now docker
sudo systemctl enable --now cron
```

Preencher no `prod.env` pelo menos:
- `POSTGRES_PASSWORD`
- `API_JWT_SECRET`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `INGEST_REQUIRE_KEY=true`
- `SEED_PASSWORD`
- `POSTGRES_SHM_SIZE`
- `POSTGRES_SHARED_BUFFERS`
- `DB_POOL_MAX_SIZE`

Política obrigatória de segurança antes do deploy:
- `API_JWT_SECRET` com 32+ caracteres e sem placeholders como `CHANGE_ME`, `default`, `password`, `admin`, `1234`.
- `CLICKHOUSE_USER` dedicado; `default` é proibido em produção/homolog/staging.
- `CLICKHOUSE_PASSWORD` forte e sem placeholders.
- `INGEST_REQUIRE_KEY=true`.

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

Validar rebuild do container API antes do smoke:

```bash
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" build api
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" up -d api
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" exec -T api python - <<'PY'
from app import schemas_bi
print([name for name in dir(schemas_bi) if name.endswith("Response")])
PY
```

Saída esperada: as classes `DashboardHomeResponse`, `SalesOverviewResponse`, `CashOverviewResponse`, `FraudOverviewResponse` e `FinanceOverviewResponse` devem aparecer no container em execução.

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
- `auth_me` deve apontar `home_path` para `/dashboard?...`, já com o recorte inicial do dia atual, e nunca para `/scope`.
- escopo sem `id_filial` explícito deve significar somente `auth.filiais` ativas e autorizadas para o usuário; filiais inativas nunca entram em `todas`.
- o gate de copy do frontend precisa estar verde no `npm test`, bloqueando jargões como `recorte`, `snapshot`, `mart`, `Saídas normais` e `Platform` como label visual.

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
- cadence provisória enquanto existir backlog ou enquanto o ambiente real ainda não tiver p95 consolidado:
  - pipeline: `*/5 * * * *`
  - risk por env default: `RISK_INTERVAL_MINUTES=30`
- cadence ideal após validar o ambiente real com o refactor delta-fino:
  - pipeline: `*/5 * * * *`
  - risk por env: `RISK_INTERVAL_MINUTES=15`
- cadence agressiva só depois de evidência nova em produção:
  - `RISK_INTERVAL_MINUTES=10-15`, apenas se o step `risk_events` mantiver folga material e sem backlog.
- decisão operacional:
  - não reativar cron de 1 minuto;
  - manter o trilho `operational` desacoplado do `risk`;
  - usar `30 min` como default seguro no instalador/pipeline e reduzir manualmente para `15 min` quando o `etl.run_log` mostrar estabilidade real.

Habilitar agent e cron somente depois dessa validação.

Instalação idempotente do cron:

```bash
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-install-cron.sh
```

## Validação funcional de vendas e acessos

Reconciliação de vendas por grupo, sem SQL manual:

```bash
TENANT_ID=1 DATE=2026-03-07 BRANCH_ID=14122 GROUP_NAME=COMBUSTIVEIS \
  docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" exec -T api \
  python -m app.cli.reconcile_sales
```

Critério:
- `totals.endpoint` deve bater com `totals.mart` e `totals.dw`;
- `deltas.legacy_bucket_extra` mostra apenas a diferença que existiria na regra antiga de bucketização;
- `legacy_bucket.extra_groups` e `legacy_bucket.extra_items` devem explicar o delta residual.

Sessão do soberano real:

```bash
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" exec -T api python - <<'PY'
import json, os, urllib.request
BASE='http://127.0.0.1:8000'
email = os.environ['PLATFORM_MASTER_EMAIL']
password = os.environ['PLATFORM_MASTER_PASSWORD']
payload=json.dumps({'identifier': email, 'password': password}).encode()
req=urllib.request.Request(BASE + '/auth/login', method='POST', headers={'Content-Type':'application/json'}, data=payload)
with urllib.request.urlopen(req, timeout=60) as response:
    login=json.loads(response.read().decode())
token=login['access_token']
me=urllib.request.Request(BASE + '/auth/me', headers={'Authorization': f'Bearer {token}'})
with urllib.request.urlopen(me, timeout=60) as response:
    body=json.loads(response.read().decode())
print(json.dumps({
    'email': body['email'],
    'user_role': body['user_role'],
    'home_path': body['home_path'],
    'platform_superuser': body['access'].get('platform_superuser'),
    'product': body['access'].get('product'),
    'platform': body['access'].get('platform'),
}, indent=2, ensure_ascii=False))
PY
```

Critério:
- `user_role = platform_master`;
- `platform_superuser = true`;
- `product = true`;
- `platform = true`.

Sessão do `master@torqmind.com`:

```bash
docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" exec -T api python - <<'PY'
import json, os, urllib.request
BASE='http://127.0.0.1:8000'
password = os.environ.get('CHANNEL_BOOTSTRAP_PASSWORD') or os.environ.get('SEED_PASSWORD')
payload=json.dumps({'email':'master@torqmind.com','password': password}).encode()
req=urllib.request.Request(BASE + '/auth/login', method='POST', headers={'Content-Type':'application/json'}, data=payload)
with urllib.request.urlopen(req, timeout=60) as response:
    login=json.loads(response.read().decode())
token=login['access_token']
me=urllib.request.Request(BASE + '/auth/me', headers={'Authorization': f'Bearer {token}'})
with urllib.request.urlopen(me, timeout=60) as response:
    body=json.loads(response.read().decode())
print(json.dumps({
    'email': body['email'],
    'user_role': body['user_role'],
    'home_path': body['home_path'],
    'platform': body['access'].get('platform'),
    'platform_finance': body['access'].get('platform_finance'),
    'product': body['access'].get('product'),
    'tenant_ids': body.get('tenant_ids'),
}, indent=2, ensure_ascii=False))
PY
```

Critério:
- `user_role = channel_admin`;
- `platform = true`;
- `platform_finance = false`;
- `product = true` somente se houver empresas ativas vinculadas ao canal bootstrap;
- `tenant_ids` deve listar apenas a carteira do canal.

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

Checklist pós-reboot do Ubuntu:

```bash
sudo systemctl is-enabled docker
sudo systemctl is-active docker
sudo systemctl is-enabled cron
sudo systemctl is-active cron
ENV_FILE="$TM_ENV" ./deploy/scripts/prod-post-boot-check.sh
```

## Rollback

1. Parar agent e cron.
2. Reverter containers para a tag estável anterior.
3. Se necessário, restaurar o dump lógico anterior:

```bash
cat backup_pre_release.dump | docker compose -f docker-compose.prod.yml --env-file "$TM_ENV" exec -T postgres \
  pg_restore -U postgres -d TORQMIND -j 4 --clean --if-exists
```
