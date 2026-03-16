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

Para acessar de outra máquina na LAN ou Radmin VPN, use o IP da máquina servidora:
- Web: `http://IP_DO_SERVIDOR:3000`
- API: `http://IP_DO_SERVIDOR:8000/docs`

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

Os containers publicam:
- Web em `0.0.0.0:3000`
- API em `0.0.0.0:8000`

Comandos úteis:

```bash
make logs   # acompanha logs
make migrate   # aplica todas as migrations em ordem
make lint   # valida build do web + compilação Python
make down   # derruba os serviços
```

### Backfill de snapshots executivos

Os snapshots históricos de `churn`, `health score` e `aging financeiro` são persistidos por `dt_ref`.
O backfill é resumível e registra progresso em:
- `app.snapshot_backfill_runs`
- `app.snapshot_backfill_steps`

Rodar um backfill inicial:

```bash
START_DT=2024-01-01 END_DT=2024-12-31 STEP_DAYS=7 ID_EMPRESA=1 make backfill-snapshots
```

Retomar um backfill interrompido:

```bash
START_DT=2024-01-01 END_DT=2024-12-31 STEP_DAYS=7 ID_EMPRESA=1 make backfill-snapshots-resume
```

---

## Seed de usuários e tenant

Depois de subir, rode:

```bash
docker compose exec api python -m app.cli.seed
```

Cria/atualiza:
- **MASTER**  → `master@torqmind.local` / valor definido em `SEED_PASSWORD`
- **OWNER**   → `owner@empresa1.local` / valor definido em `SEED_PASSWORD`  (Empresa 1)
- **MANAGER** → `manager@empresa1.local` / valor definido em `SEED_PASSWORD` (Empresa 1, Filial 1)

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
- `formas_pgto_comprovantes`
- `comprovantes`
- `contaspagar`
- `contasreceber`
- `financeiro`

### Configuração segura do Agent

No diretório `apps/agent`, use sempre:

```bash
cp config.example.yaml config.local.yaml
```

Em produção Windows, o diretório final do cliente deve conter apenas `config.enc`.  
Use YAML apenas para desenvolvimento local ou migração para `config.enc`.

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
- valor atual de `SEED_PASSWORD` no seu `.env` local

### Frontend remoto, LAN e Radmin VPN
O frontend não deve apontar para `localhost` quando aberto em outra máquina. A configuração agora funciona assim:

- `NEXT_PUBLIC_API_URL`: URL pública fixa da API. Deixe vazia para o navegador usar o mesmo hostname da página atual com porta `8000`.
- `API_INTERNAL_URL`: URL interna usada pelo container do Next.js em chamadas server-side. Em Docker, o default correto é `http://api:8000`.
- `NEXT_PUBLIC_API_PORT`: porta pública usada no fallback automático do navegador. Default `8000`.
- `APP_CORS_ORIGINS`: origens explícitas permitidas, por padrão `http://localhost:3000,http://127.0.0.1:3000`.
- `APP_CORS_ORIGIN_REGEX`: regex para permitir acesso por hostname/IP na porta `3000`, cobrindo LAN e Radmin VPN sem hardcode de IP.

Exemplos:

- Desenvolvimento local na mesma máquina: acesse `http://localhost:3000`
- Outra máquina na LAN: acesse `http://192.168.x.y:3000`
- Outra máquina via Radmin VPN: acesse `http://IP_RADMIN:3000`

Se quiser forçar uma URL pública fixa da API, defina no `.env`:

```bash
NEXT_PUBLIC_API_URL=http://192.168.x.y:8000
```

Portas que precisam estar acessíveis na máquina servidora:
- `3000/tcp` para o frontend
- `8000/tcp` para a API
