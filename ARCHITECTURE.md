Markdown
# TorqMind - Architecture & AI Context

## 1. Contexto do Produto
[cite_start]O TorqMind é um micro SaaS multi-tenant de gestão operacional, BI e inteligência para postos de combustíveis[cite: 2]. [cite_start]O produto atende empresas com múltiplas filiais e precisa ser extremamente confiável para a operação diária[cite: 3]. [cite_start]Este não é um projeto demo; tudo deve ser tratado como software de produção com impacto real[cite: 4].

## 2. Stack Tecnológica
[cite_start]Projeto estruturado como um Monorepo[cite: 5]:
- **Frontend (`apps/web`)**: Next.js 14 + TypeScript. [cite_start]Possui dashboards operacionais, de vendas, antifraude, financeiro, metas e backoffice[cite: 5, 7].
- **Backend (`apps/api`)**: FastAPI + Pydantic + JWT. [cite_start]Expõe autenticação, ingestão NDJSON, ETL e endpoints de BI[cite: 5, 6].
- **Dados**: PostgreSQL (`sql/migrations` — OLTP/STG/DW/mash) + ClickHouse (`sql/clickhouse` — leitura BI).
- **Streaming**: Debezium + Redpanda + `apps/cdc_consumer`.
- [cite_start]**Infra (`deploy`)**: Docker Compose multi-VM, scripts de produção e nginx[cite: 5].

## 3. Modelo de Dados e Pipeline

**Estado atual (hot path BI):** ClickHouse-first.

```
stg/dw (PG) → CDC/ETL/publish → torqmind_mart_rt / torqmind_mart (CH) → API → Web
```

- `stg.*` = landing/raw (dados brutos)
- `dw.*` = dimensões e fatos consolidados (mash ETL)
- `mart.*` (PostgreSQL) = staging de mash / publicação / legado — **não** é a fonte padrão de resposta dos dashboards
- ClickHouse `torqmind_mart_rt` / `torqmind_mart` = leitura analítica canônica da API para o frontend
- Exceções PG analíticas: só se registradas em `apps/api/app/analytics_pg_exceptions.json`

Autoridade: `AGENTS.md`, `CODEX_TORQMIND_MAP.md`, `.cursor/rules/06-clickhouse-bi-reads.mdc`.

> **Histórico:** menções a “dashboard lê exclusivamente `mart.agg_*` PG” (ex.: incidente 2026-04 na seção 8) descrevem a correção que eliminou `dw.fact_*` do overlay. Isso **não** é o hot path atual após o cutover realtime ClickHouse.

## 4. Regras de Ouro (Multi-tenant e Segurança)
- **Isolamento Absoluto:** O sistema é multi-tenant. [cite_start]Use `id_empresa` e, frequentemente, `id_filial` como chaves de escopo obrigatórias em todas as queries e lógicas[cite: 9]. [cite_start]Nunca vaze dados entre tenants[cite: 11].
- [cite_start]**Permissões:** O acesso é influenciado pelo estado da empresa e filial[cite: 13]. [cite_start]Permissões de platform diferem das permissões do tenant[cite: 12].

## 5. Diretrizes para Agentes de IA (CRÍTICO)
- [cite_start]**Sem Hacks:** Preserve os padrões existentes[cite: 14]. [cite_start]Não use placeholders, mocks falsos ou hacks temporários[cite: 16].
- **O Incidente Risk_v2:** Recentemente, uma refatoração no ETL de risco falhou por ignorar o grão da tabela e tentar mesclar eventos de usuário com eventos de comprovante, causando conflitos na constraint de unicidade e triplos scans de tabela. **NUNCA presuma a estrutura do banco.** Sempre verifique os schemas, constraints e migrations reais antes de escrever SQL.
- [cite_start]**Workflow Exigido:** Inspecione os módulos antes de editar [cite: 15][cite_start], mantenha as mudanças pequenas e legíveis [cite: 15][cite_start], atualize os testes [cite: 17] [cite_start]e reporte os arquivos alterados com os riscos remanescentes[cite: 18].

## 6. Schema Reference Quente (verdade absoluta — fonte: migrations 003 + 004)

### `dw.fact_comprovante` (grão = comprovante)
- **PK:** (id_empresa, id_filial, id_db, id_comprovante)
- **Campos chave:** id_usuario, id_turno, id_cliente, **`valor_total` numeric(18,2)**, cancelado boolean, situacao int, data, data_key int (YYYYMMDD)

### `dw.fact_venda` (grão = movimento de produto)
- **PK:** (id_empresa, id_filial, id_db, id_movprodutos)
- **Campos chave:** id_usuario, id_cliente, id_comprovante, id_turno, saidas_entradas int, **`total_venda` numeric(18,2)** (SINGULAR — não confundir com `total_vendas` usado em mart.* views), cancelado, payload jsonb

### `dw.fact_venda_item` (grão = item)
- **PK:** (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
- **Campos chave:** id_produto, id_grupo_produto, id_local_venda, id_funcionario, cfop, qtd, valor_unitario, total, desconto, custo_total, margem

### `dw.fact_risco_evento` (grão = comprovante OU sintético)
- **PK:** id (bigserial)
- **Constraint:** `uq_fact_risco_evento_nk = (id_empresa, id_filial, event_type, id_db_nk, id_comprovante_nk, id_movprodutos_nk)`
- **`*_nk` são `GENERATED ALWAYS AS COALESCE(col, -1) STORED`** — qualquer NULL colapsa para -1.
- **REGRA CRÍTICA:** múltiplos eventos do mesmo `event_type` com IDs todos NULL violam a unique (todos colapsam para `(-1,-1,-1)`). Use IDs sintéticos negativos por chave lógica:
  - `FUNCIONARIO_OUTLIER`: `id_db = -1`, `id_comprovante = -id_usuario`, `id_movprodutos = -data_key`
- **Event types existentes:** `CANCELAMENTO`, `FUNCIONARIO_OUTLIER`, `DESCONTO_OUTLIER` (e variantes em migration 031)
- **score_level:** ALTO (≥80), SUSPEITO (≥60), ATENCAO (≥40), NORMAL (<40)

## 7. Função `etl.compute_risk_events_v2` — Contrato (atual, migration 062)
- **Assinatura:** `(p_id_empresa int, p_force_full boolean DEFAULT false, p_lookback_days int DEFAULT 14, p_end_ts timestamptz DEFAULT NULL) RETURNS integer`
- **Padrão:** single-scan em `fact_comprovante` via `cte_base AS MATERIALIZED` → derivadas (`p90_cancel`, `user_stats`) → 2 streams de eventos:
  - `cancel_events` (grão real de comprovante)
  - `outlier_events` (grão sintético: `id_db=-1, id_comprovante=-id_usuario, id_movprodutos=-data_key`)
  → `INSERT ... ON CONFLICT ON CONSTRAINT uq_fact_risco_evento_nk DO UPDATE`.
- **Watermark:** `etl.set_watermark(empresa,'risk_events', now())` ao final.
- **Call-sites:** `apps/api/app/routes_etl.py` (micro_risk), `apps/api/app/services/etl_orchestrator.py` (cycle), `apps/api/app/test_release_hardening.py`.

## 8. Histórico de Incidentes (evidência — não lei de arquitetura atual)

> As entradas desta seção são **históricas**. O hot path BI atual é ClickHouse (`repos_mart_realtime` / `torqmind_mart_rt`), não “ler só `mart.agg_*` PostgreSQL”.

- **2026-04 — Risk_v2 broken:** v2 inicial (migration 059) inseria `FUNCIONARIO_OUTLIER` com todos os IDs NULL → `*_nk` colapsavam para `(-1,-1,-1)` → violação de `uq_fact_risco_evento_nk` quando havia mais de 1 outlier no dia/filial. Adicionalmente, fazia 3 scans em `fact_comprovante`. **Resolvido em migration 062** (IDs sintéticos + CTE base materializada).
- **Confusão `total_venda` vs `total_vendas`:** o campo real em `dw.fact_venda` é `total_venda` (singular). Views `mart.*` (ex.: `mart.fato_caixa_diario`) podem expor o agregado como `total_vendas` (plural). A função `compute_risk_events_v2` **NÃO usa `fact_venda`** — opera apenas em `fact_comprovante.valor_total`.
- **2026-04-27 a 2026-05-05 — `fact_estoque_atual` ausente:** o pipeline `etl-operational` falhava no step 14 com `function etl.load_fact_estoque_atual(smallint) does not exist`. **Resolvido na migration 074**, que cria `stg.estoque`, `dw.fact_estoque_atual`, `etl.load_fact_estoque_atual` e `mart.agg_estoque_posicao_atual`, reativando o step de estoque no orchestrator e no teste de ordem.
- **2026-04-29 — Dashboard Geral stale + lentidão (3min/1dia):** Duas fases de correção **na época**:
  - **Fase 1 — ETL:** track `etl-operational` não invocava `etl.refresh_marts`. Resolvido alterando `_track_runs_publication` para incluir `TRACK_OPERATIONAL`. Flag `publication_deferred` congelada em `False`.
  - **Fase 2 — Eliminação de `dw.fact_*` do Dashboard:** remoção do overlay “dia ao vivo” que escaneava `dw.fact_*`. Naquele momento o dashboard passou a ler agregados `mart.agg_*` PG. **Depois** veio o cutover ClickHouse-first: a leitura canônica de dashboard/overview é CH via `repos_analytics` / `repos_mart_realtime` (ver seção 3 e `AGENTS.md`). Não reintroduzir fatos no hot path.

## 9. ETL — Mapa de Steps Operacionais (PHASE_SQL_STEPS)
Localização: `apps/api/app/services/etl_orchestrator.py` (~linha 74). Steps executados sequencialmente em `_run_tenant_phase` para a track operacional. `step_count` é dinâmico: `len(PHASE_SQL_STEPS) + int(track_runs_risk)`.

| # | Step | Função SQL |
|---|---|---|
| 1 | dim_filial | `etl.load_dim_filial` |
| 2 | dim_grupos | `etl.load_dim_grupos` |
| 3 | dim_localvendas | `etl.load_dim_localvendas` |
| 4 | dim_produtos | `etl.load_dim_produtos` |
| 5 | dim_funcionarios | `etl.load_dim_funcionarios` |
| 6 | dim_usuario_caixa | `etl.load_dim_usuario_caixa` |
| 7 | dim_clientes | `etl.load_dim_clientes` |
| 8 | fact_comprovante | `etl.load_fact_comprovante` |
| 9 | fact_caixa_turno | `etl.load_fact_caixa_turno` |
| 10 | fact_pagamento_comprovante | `etl.load_fact_pagamento_comprovante` |
| 11 | fact_venda | `etl.load_fact_venda` |
| 12 | fact_venda_item | `etl.load_fact_venda_item` |
| 13 | fact_financeiro | `etl.load_fact_financeiro` |
| 14 | fact_estoque_atual | `etl.load_fact_estoque_atual` (migration 074) |
| risk | risk_events (track risk) | `etl.compute_risk_events_v2` (migration 062) |

## 10. Política de Refresh por Track (`etl_orchestrator._track_runs_publication`)

> **Contexto histórico (pós 2026-04-29).** No cutover ClickHouse-first atual, `etl.refresh_marts` PG legado pode estar desligado; freshness da tela vem de publish/CDC CH. Validar flags e `mart_publication_log` — não assumir que MV PG está fresca só porque a track rodou.

Após 2026-04-29 (incidente Dashboard Geral), o mapa de tracks era:

| Track | Per-tenant phase | Fast-path post-refresh | Global `etl.refresh_marts` |
|---|---|---|---|
| `operational` | ✅ | ✅ | ✅ (NOVO) |
| `risk` | (skip) | ✅ | ✅ |
| `full` | ✅ | ✅ | ✅ |

- `_track_runs_publication(track)` retorna `True` para `{OPERATIONAL, RISK, FULL}`.
- A flag `publication_deferred` (item `meta`) está congelada em `False` no novo regime — chave preservada apenas para compatibilidade do schema do summary.
- Cron `prod-etl-operational.sh` / tracks acima são o mapa **histórico** de refresh PG. No cutover atual, validar publish CH / `mart_publication_log` e flags realtime — não assumir que `etl.refresh_marts` PG alimenta a tela.