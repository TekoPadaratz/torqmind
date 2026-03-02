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

## Fluxo reprodutível no WSL (do zero)

Pré-requisitos:
- Docker Desktop com integração WSL habilitada
- `docker compose` disponível no terminal

Passo a passo:

1) Setup inicial:

```bash
make setup
```

2) Subir serviços:

```bash
make up
```

3) Rodar migrações (reset/migration local):

```bash
make migrate
```

4) Rodar testes:

```bash
make test
```

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

Isso gera dados sintéticos em `stg.*` e executa `etl.run_all(1, true)`.

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
- `/goals` → Metas & Equipe

---

## Reset do banco (dev/homolog)

Você pode rodar o script:

- `sql/torqmind_reset_db_v2.sql`

Ele recria schemas/tabelas/functions/views/materialized views e seeds mínimos.

> **Atenção:** ele faz `DROP SCHEMA ... CASCADE` (não use em produção).
