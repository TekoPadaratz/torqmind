# TorqMind — Documento de Contexto do Projeto

> Este arquivo é lido automaticamente pelo Cursor Agent para manter contexto do projeto.
> Mantenha-o atualizado conforme o sistema evolui.
> Hierarquia de autoridade: `AGENTS.md` → `CODEX_TORQMIND_MAP.md` → contrato de produto → `.cursor/rules/*`.

## O que é o TorqMind

Micro SaaS Multi-Tenant de BI e Gestão Operacional para redes de postos de combustíveis.
Software de produção. Impacto real. Clientes reais com múltiplas filiais.

**Módulos de negócio:**
- Dashboard Geral (KPIs executivos)
- Vendas (análise por produto, turno, filial)
- Antifraude / Risco (motor risk_v2)
- Financeiro (DRE, margem, custo)
- Gestão de Lucro / Produto / Metas / Equipe / Preços
- Clientes / Churn (RFM, retenção)
- Caixa (fluxo, conciliação)
- Inventário / Perda de combustível
- Backoffice Platform (empresas, filiais, usuários, contratos, recebíveis, auditoria)

## Decisões Arquiteturais Críticas

### Hot path BI = ClickHouse (não PostgreSQL `mart.*`)

Telas e endpoints de dashboard/BI leem **ClickHouse** via facade `repos_analytics` → `repos_mart_realtime` (`torqmind_mart_rt` / `torqmind_mart`), com cutover `USE_REALTIME_MARTS=true` / `USE_CLICKHOUSE=true` / `REALTIME_MARTS_FALLBACK=false` em produção.

```
SQL Server Xpert → Agent → API /ingest → PostgreSQL STG/DW
                         → Debezium → Redpanda → cdc_consumer
                         → ClickHouse (current / mart_rt / mart)
                         → API BI → Next.js
```

Contrato canônico: `.cursor/rules/06-clickhouse-bi-reads.mdc` e `AGENTS.md`.

### Papel do PostgreSQL

| Camada | Papel atual |
|---|---|
| `app.*` / `auth.*` | OLTP (usuários, ACL, branding, configs) |
| `stg.*` | Landing/raw da ingestão — auditável; não apagar |
| `dw.*` | Fatos/dimensões consolidadas (ETL mash) |
| `mart.*` (PG) | Staging de mash / publicação / legado — **não** é o hot path silencioso do dashboard |
| `etl.*` | Controle, watermarks, funções PL/pgSQL |

Exceções analíticas que ainda leem PostgreSQL devem estar **registradas** em `apps/api/app/analytics_pg_exceptions.json` (CI bloqueia nova exceção sem registry). Detalhe: `06-clickhouse-bi-reads.mdc`.

### Por que não ler `dw.fact_*` no Dashboard

Consultar `dw.fact_venda` (e overlays “ao vivo” em fatos) no hot path da tela causou lentidão da ordem de minutos. A correção histórica foi materializar agregados; a arquitetura **atual** serve esses agregados a partir do ClickHouse, não do PostgreSQL `mart.*` como fonte de resposta da API de BI.

`repos_mart.py` permanece como repositório/legado PG e apoio a mash/exceções — não reintroduzir leitura de `dw.fact_*` em overview/dashboard.

### Por que ETL incremental?

Bases grandes travam quando o ETL reprocessa demais.
O `etl_orchestrator.py` controla o que foi processado via tabelas `etl.*`.
Cargas novas chegam continuamente — o pipeline precisa ser robusto a isso.

### Por que ID sintético negativo no risk_v2?

O grão de `dw.fact_risco_evento` é por `id_comprovante`.
Usuários outliers não têm comprovante válido → constraint `uq_fact_risco_evento_nk` quebra.
Solução: ID sintético negativo para esses casos.

### Homolog ≠ isolado no analytics

Homolog e Prod compartilham ClickHouse/CDC em `172.30.0.9`. DDL slim/mart e rebuild do `cdc-consumer` são mudança de produção. Ver `AGENTS.md`.

## Pontos de Atenção Ativos

- ETL incremental deve terminar rápido — cron de produção não pode travar
- Filiais são administráveis via backoffice — ETL não pode reverter essas mudanças
- Migrations: inventário em `sql/migrations/MANIFEST.json` — sempre verifique a migration mais recente antes de assumir schema
- Coluna em `dw.fact_venda`: `total_venda` (singular). Agregados/marts podem expor `total_vendas` (plural) — não confundir
- Documento operacional = NF-e/NFC-e (nunca `NROCOMPROVANTE` / `id_comprovante`)

## Ambiente de Desenvolvimento

```bash
# Setup inicial (local) — nunca confundir com Hom/Prod
docker compose up -d

# Operação diária (local)
make migrate          # aplica migrations pendentes
make test             # testes
make lint             # linting
make etl-incremental  # ciclo ETL manual
```

Homolog/Prod: compose com `-p` / `-f` / `--env-file` / serviço exato. Ver `.cursor/rules/10-docker-isolation.mdc`.

## Arquivos-Chave

| Arquivo | Responsabilidade |
|---|---|
| `AGENTS.md` | Operação, segurança, dados canônicos, PASS |
| `CODEX_TORQMIND_MAP.md` | Mapa técnico vivo |
| `apps/api/app/repos_analytics.py` | Facade BI (escolhe realtime/CH/PG) |
| `apps/api/app/repos_mart_realtime.py` | Hot path ClickHouse |
| `apps/api/app/repos_mart.py` | Repositório PG legado / mash / exceções |
| `apps/api/app/services/etl_orchestrator.py` | Orquestrador central do ETL |
| `apps/api/app/analytics_pg_exceptions.json` | Dívidas PG analíticas registradas |
| `sql/migrations/` | Fonte da verdade do schema PostgreSQL |
| `sql/clickhouse/` | DDL/streaming ClickHouse |
| `.cursor/rules/` | Regras de execução do Cursor |
