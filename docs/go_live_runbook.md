# TorqMind Go-Live Runbook (Executável)

Data: 2026-03-03
Objetivo: executar deploy com validação ponta a ponta e rollback rápido.

## Pré-requisitos

- Acesso shell ao host com Docker.
- Permissão para rodar `docker compose` e `psql` no container postgres.
- Arquivo `.env` com segredos de produção.

Variáveis úteis:

```bash
export TM_ROOT=/home/eko/projects/TorqMind
cd "$TM_ROOT"
```

## T-24h (Homologação final)

```bash
cd "$TM_ROOT"
make ci
```

Esperado:
- API smoke `Ran 5 tests ... OK`
- Agent tests `Ran 14 tests ... OK`
- Front build/typecheck OK

Backup lógico (obrigatório):

```bash
docker compose exec -T postgres pg_dump -U postgres -d TORQMIND > backup_pre_release_$(date +%Y%m%d_%H%M%S).sql
ls -lh backup_pre_release_*.sql | tail -n1
```

## T-1h (baseline operacional)

```bash
docker compose ps
docker compose logs --tail=120 api
```

```bash
docker compose exec -T api python - <<'PY'
import urllib.request, json
u='http://localhost:8000/health'
with urllib.request.urlopen(u, timeout=60) as r:
    print(json.dumps(json.loads(r.read().decode()), indent=2, ensure_ascii=False))
PY
```

Critério: `ok=true` e stack estável.

## T-15min (deploy)

### 1) Aplicar migrations

```bash
docker compose exec -T postgres psql -U postgres -d TORQMIND -v ON_ERROR_STOP=1 < sql/migrations/006_ingest_shadow_columns.sql
docker compose exec -T postgres psql -U postgres -d TORQMIND -v ON_ERROR_STOP=1 < sql/migrations/007_etl_incremental_hot_received.sql
docker compose exec -T postgres psql -U postgres -d TORQMIND -v ON_ERROR_STOP=1 < sql/migrations/008_run_all_skip_risk_when_no_changes.sql
docker compose exec -T postgres psql -U postgres -d TORQMIND -v ON_ERROR_STOP=1 < sql/migrations/009_phase4_moneyleak_health.sql
docker compose exec -T postgres psql -U postgres -d TORQMIND -v ON_ERROR_STOP=1 < sql/migrations/010_phase5_ai_engine.sql
docker compose exec -T postgres psql -U postgres -d TORQMIND -v ON_ERROR_STOP=1 < sql/migrations/011_phase7_notifications.sql
```

### 2) Subir imagens

```bash
docker compose up -d --build api web
docker compose ps
```

### 3) Smoke de endpoints críticos

```bash
docker compose exec -T api python - <<'PY'
import json, urllib.request
BASE='http://localhost:8000'

def req(path, method='GET', data=None, token=None):
    h={'Content-Type':'application/json'}
    if token: h['Authorization']=f'Bearer {token}'
    b=None if data is None else json.dumps(data).encode()
    r=urllib.request.Request(BASE+path, method=method, data=b, headers=h)
    with urllib.request.urlopen(r, timeout=180) as resp:
        return resp.status, json.loads(resp.read().decode())

_, login = req('/auth/login','POST',{'email':'owner@empresa1.com','password':'TorqMind@123'})
t=login['access_token']
checks={
  'dashboard': req('/bi/dashboard/overview?dt_ini=2026-03-01&dt_fim=2026-03-03&id_empresa=1', token=t),
  'churn': req('/bi/clients/churn?dt_ini=2026-03-01&dt_fim=2026-03-03&id_empresa=1&min_score=40&limit=5', token=t),
  'finance': req('/bi/finance/overview?dt_ini=2026-03-01&dt_fim=2026-03-03&id_empresa=1', token=t),
  'jarvis': req('/bi/jarvis/generate?dt_ref=2026-03-02&id_empresa=1&limit=5&force=true','POST',{},t),
  'notif': req('/bi/notifications?id_empresa=1&limit=10', token=t),
}
print(json.dumps({k:v[0] for k,v in checks.items()}, indent=2))
PY
```

Critério: todos os status `200`.

## T+1h (validação ponta a ponta)

### 1) Ingest + ETL 2x + endpoints + contagens

```bash
docker compose exec -T api python - <<'PY'
import json, time, urllib.request
from datetime import datetime, timezone
import psycopg
BASE='http://localhost:8000'

def req(path, method='GET', data=None, headers=None):
    h={'Content-Type':'application/json'}
    if headers: h.update(headers)
    b=None if data is None else json.dumps(data).encode('utf-8')
    r=urllib.request.Request(BASE+path, method=method, headers=h, data=b)
    with urllib.request.urlopen(r, timeout=240) as resp:
        return resp.status, json.loads(resp.read().decode('utf-8'))

_, login = req('/auth/login','POST',{'email':'owner@empresa1.com','password':'TorqMind@123'})
token = login['access_token']
auth={'Authorization':f'Bearer {token}'}

conn = psycopg.connect('host=postgres port=5432 dbname=TORQMIND user=postgres password=1234')
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute('SELECT ingest_key::text FROM app.tenants WHERE id_empresa=1')
    ingest_key = cur.fetchone()[0]

uid = int(datetime.now(tz=timezone.utc).timestamp()) % 2000000000
line={'ID_FILIAL':1,'ID_PRODUTO':uid,'NOME':f'RUNBOOK PROD {uid}'}
raw=(json.dumps(line, ensure_ascii=False)+'\n').encode('utf-8')
r = urllib.request.Request(BASE+'/ingest/produtos', method='POST', headers={'X-Ingest-Key': ingest_key, 'Content-Type':'application/x-ndjson'}, data=raw)
with urllib.request.urlopen(r, timeout=240) as resp:
    ingest_resp=json.loads(resp.read().decode())

with conn.cursor() as cur:
    t1=time.perf_counter(); cur.execute('SELECT etl.run_all(%s,%s,%s)', (1,False,True)); cur.fetchone(); e1=(time.perf_counter()-t1)*1000
    t2=time.perf_counter(); cur.execute('SELECT etl.run_all(%s,%s,%s)', (1,False,True)); cur.fetchone(); e2=(time.perf_counter()-t2)*1000

checks={
  'dashboard': req('/bi/dashboard/overview?dt_ini=2026-03-01&dt_fim=2026-03-03&id_empresa=1', headers=auth)[0],
  'churn': req('/bi/clients/churn?dt_ini=2026-03-01&dt_fim=2026-03-03&id_empresa=1&min_score=40&limit=5', headers=auth)[0],
  'finance': req('/bi/finance/overview?dt_ini=2026-03-01&dt_fim=2026-03-03&id_empresa=1', headers=auth)[0],
  'notifications': req('/bi/notifications?id_empresa=1&limit=10', headers=auth)[0],
}
print(json.dumps({'ingest_ok':ingest_resp.get('ok'),'etl_ms_run1':round(e1,2),'etl_ms_run2':round(e2,2),'status':checks}, indent=2))
conn.close()
PY
```

```bash
docker compose exec -T postgres psql -U postgres -d TORQMIND -P pager=off -c "
SELECT 'stg.comprovantes' AS tabela, COUNT(*) AS total FROM stg.comprovantes WHERE id_empresa=1
UNION ALL SELECT 'stg.movprodutos', COUNT(*) FROM stg.movprodutos WHERE id_empresa=1
UNION ALL SELECT 'stg.itensmovprodutos', COUNT(*) FROM stg.itensmovprodutos WHERE id_empresa=1
UNION ALL SELECT 'dw.fact_comprovante', COUNT(*) FROM dw.fact_comprovante WHERE id_empresa=1
UNION ALL SELECT 'dw.fact_venda', COUNT(*) FROM dw.fact_venda WHERE id_empresa=1
UNION ALL SELECT 'dw.fact_venda_item', COUNT(*) FROM dw.fact_venda_item WHERE id_empresa=1
UNION ALL SELECT 'mart.agg_vendas_diaria', COUNT(*) FROM mart.agg_vendas_diaria WHERE id_empresa=1
UNION ALL SELECT 'mart.agg_vendas_hora', COUNT(*) FROM mart.agg_vendas_hora WHERE id_empresa=1
UNION ALL SELECT 'app.notifications', COUNT(*) FROM app.notifications WHERE id_empresa=1
ORDER BY tabela;
"
```

## T+24h (aceite)

```bash
make ci
```

Checklist funcional manual (front):
- Dashboard mostra HERO monetário.
- Top 3 ações com checklist e evidências.
- Radares Fraude/Churn/Caixa carregados.
- Alertas in-app aparecem e podem ser marcados como lidos.

## Rollback (se incidente)

1. Reverter containers para tag anterior estável.

```bash
# Exemplo: ajustar imagens no compose para tag anterior e subir
# docker compose up -d api web
```

2. Desativar IA para fallback determinístico:

```bash
# remover OPENAI_API_KEY do env da API e reiniciar api
# docker compose up -d --build api
```

3. Se necessário, restaurar backup:

```bash
# psql restore (planejar janela de indisponibilidade)
# psql -U postgres -d TORQMIND < backup_pre_release_YYYYMMDD_HHMMSS.sql
```
