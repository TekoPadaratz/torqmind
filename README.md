# TorqMind Monorepo (Web + API + DW)

Este repositório entrega um **BI multi-tenant** com:
- `apps/api`  : **FastAPI** (Auth JWT, ingest NDJSON, ETL STG→DW→MART, endpoints BI)
- `apps/web`  : **Next.js** (dashboards: geral, vendas, anti-fraude, clientes, financeiro, metas)
- `sql/migrations`: scripts de inicialização do Postgres (schemas, tabelas, ETL SQL e materialized views)
- `sql/torqmind_reset_db_v2.sql`: script único de reset completo (para dev / homolog)

---

## Rodar local com Docker

1) Garanta que existe um arquivo `.env` na raiz (já vem pronto neste pacote).

2) Suba tudo:

```bash
docker compose up --build
```

Acesse:
- Web: http://localhost:3000
- API: http://localhost:8000/docs
- Postgres: localhost:5432

---

## Fluxo rápido local (3 comandos)

Pré-requisitos:
- Docker Desktop com integração WSL habilitada
- `docker compose` disponível no terminal

1) Subir stack:
```bash
docker compose up --build -d
```

2) Seed de usuários + tenant:
```bash
docker compose exec api python -m app.cli.seed
```

3) Carga demo (ingest STG + ETL STG→DW→MART):
```bash
docker compose exec api python -m app.cli.demo_load
```

Depois, acesse:
- Web: http://localhost:3000
- API docs: http://localhost:8000/docs
- Health: http://localhost:8000/health
- Debug DB (dev): http://localhost:8000/debug/db

Comandos úteis:

```bash
make logs   # acompanha logs
make lint   # valida build do web + compilação Python
make down   # derruba os serviços
```

---

## Seed de usuários e tenant

Depois de subir, rode:

```bash
docker compose exec api python -m app.cli.seed
```

Cria/atualiza:
- **MASTER**  → `master@torqmind.local` / `TorqMind@123`
- **OWNER**   → `owner@empresa1.local` / `TorqMind@123`  (Empresa 1)
- **MANAGER** → `manager@empresa1.local` / `TorqMind@123` (Empresa 1, Filial 1)

E imprime o `ingest_key` da Empresa 1 (útil para o Agent).

---

## Carregar dados demo (dashboards cheios)

Se você quiser ver os dashboards funcionando imediatamente (sem integrar SQL Server ainda):

```bash
docker compose exec api python -m app.cli.demo_load
```

Isso gera dados sintéticos em `stg.*` e executa `etl.run_all(1, true, true)`.

---

## Ingestão (NDJSON)

Endpoint:
- `POST /ingest/{dataset}`

Headers recomendados:
- `X-Ingest-Key: <uuid>`  (mapeia para `app.tenants.ingest_key`)

Datasets suportados:
- `filiais`
- `funcionarios`
- `clientes` / `entidades`
- `produtos`
- `grupoprodutos`
- `localvendas`
- `turnos`
- `movprodutos`
- `itensmovprodutos`
- `comprovantes`
- `contaspagar`
- `contasreceber`
- `financeiro`

---

## Jarvis IA (Responses API) com custo controlado

Endpoints:
- `POST /bi/jarvis/generate?dt_ref=YYYY-MM-DD&id_filial=&id_empresa=&limit=10&force=false`
- `GET /bi/admin/ai-usage?days=30&id_filial=&id_empresa=`

Política:
- IA roda apenas nos top N insights por impacto (configurável via `JARVIS_AI_TOP_N`).
- Cache por hash em `app.insight_ai_cache` para evitar chamadas repetidas.
- Fallback determinístico automático quando a API de IA falhar ou não estiver configurada.

Variáveis de ambiente relevantes:
- `OPENAI_API_KEY`
- `JARVIS_MODEL_FAST` (default `gpt-4.1-mini`)
- `JARVIS_MODEL_STRONG` (default `gpt-4.1`)
- `JARVIS_AI_TOP_N` (default `10`)
- `JARVIS_AI_MAX_OUTPUT_TOKENS` (default `500`)

Pricing:
- custo é por token e varia por modelo ao longo do tempo; mantenha os coeficientes via env:
  - `JARVIS_AI_INPUT_COST_PER_1M`
  - `JARVIS_AI_OUTPUT_COST_PER_1M`

Parâmetros:
- `run_etl=true` (opcional)
- `refresh_mart=true` (se `run_etl=true`)

---

## Dashboards (Web)

A tela `/scope` define `dt_ini`, `dt_fim`, `id_empresa` (MASTER) e `id_filial` (opcional).

Páginas:
- `/dashboard` → Dashboard Geral + Jarvis briefing
- `/sales` → Vendas & Stores
- `/fraud` → Sistema Anti-Fraude
- `/customers` → Análise de Clientes
- `/finance` → Financeiro
- `/pricing` → Preço da Concorrência (input manual + simulação 10 dias)
- `/goals` → Metas & Equipe

---

## Reset do banco (dev/homolog)

Você pode rodar o script:

- `sql/torqmind_reset_db_v2.sql`

Ele recria schemas/tabelas/functions/views/materialized views e seeds mínimos.

> **Atenção:** ele faz `DROP SCHEMA ... CASCADE` (não use em produção).

---

## Troubleshooting

### Tabelas vazias no pgAdmin
Na maioria dos casos, API/CLI e pgAdmin estão apontando para bancos diferentes.

Cheque:
1) `DATABASE_URL` e `PG_*` no container `api`
2) `docker compose exec api curl -s http://localhost:8000/debug/db`
3) conexão do pgAdmin (host/porta/db/usuário)

O endpoint `/debug/db` deve bater com o mesmo banco que você abriu no pgAdmin.

---

## Release e validação final

- Release notes operacionais: `docs/release_notes.md`
- Proof pack técnico (comandos, tempos ETL, contagens, endpoints): `docs/proof_pack.md`

### Login falhando com 422 / erro estranho no front
O frontend agora converte erros da API em texto; verifique resposta em:
- `http://localhost:8000/docs` (endpoint `/auth/login`)
- senha do seed: `TorqMind@123` (ou `SEED_PASSWORD` no `.env`)
