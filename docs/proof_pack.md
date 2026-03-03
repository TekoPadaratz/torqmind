# TorqMind Proof Pack (Fases 0-5)

Data: 2026-03-03

## Comandos executados

- `docker compose ps`
- `docker compose logs --tail=200 api`
- `docker compose exec -T postgres psql -U postgres -d TORQMIND -c "\\dn"`
- `docker compose exec -T postgres psql -U postgres -d TORQMIND -c "\\dt stg.*"`
- `docker compose exec -T postgres psql -U postgres -d TORQMIND -c "\\dt dw.*"`
- `docker compose exec -T postgres psql -U postgres -d TORQMIND -c "SELECT ... counts ..."`
- `docker compose exec -T postgres psql -U postgres -d TORQMIND < sql/migrations/006_ingest_shadow_columns.sql`
- `docker compose exec -T postgres psql -U postgres -d TORQMIND < sql/migrations/007_etl_incremental_hot_received.sql`
- `docker compose exec -T postgres psql -U postgres -d TORQMIND < sql/migrations/008_run_all_skip_risk_when_no_changes.sql`
- `docker compose exec -T postgres psql -U postgres -d TORQMIND < sql/migrations/009_phase4_moneyleak_health.sql`
- `docker compose exec -T postgres psql -U postgres -d TORQMIND < sql/migrations/010_phase5_ai_engine.sql`
- `docker compose up -d --build api`
- `docker compose up -d --build api web`
- `make test`
- `make lint`
- `cd apps/agent && python3 -m unittest discover -s tests -v`

## Contagens STG/DW/MART

- `stg.comprovantes`: 10,924,419
- `stg.movprodutos`: 10,909,620
- `stg.itensmovprodutos`: 18,546,845
- `dw.fact_comprovante`: 10,924,419
- `dw.fact_venda`: 10,909,620
- `dw.fact_venda_item`: 18,546,845
- `mart.agg_vendas_diaria`: 17,645
- `mart.agg_vendas_hora`: 335,407
- `mart.customer_churn_risk_daily`: 1,362
- `mart.finance_aging_daily`: 23
- `mart.health_score_daily`: 23
- `app.insights_gerados`: 32
- `app.insight_ai_cache`: 1

## ETL (1ª/2ª execução)

Tenant de smoke (`id_empresa=999`, `etl.run_all(false,false)`):

- 1ª execução: `798.49 ms`
- 2ª execução: `783.59 ms`

Tenant real (`id_empresa=1`, `etl.run_all(false,true)`):

- execução #1: `39,023 ms`
- execução #2: `41,658 ms`
- `risk_events_skipped=true` (sem mudanças em facts)

## Endpoints respondendo

- `GET /bi/dashboard/overview?...id_empresa=1` -> `200`
- `GET /ingest/health` -> `200` (`datasets=14`)
- `POST /ingest/produtos` -> `200` com estatísticas:
  - `inserted_or_updated=1`
  - `inserted=1`
  - `updated=0`

Fase 4 (novos endpoints):

- `GET /bi/clients/churn?...&min_score=40` -> `200` (`top_risk` + `drilldown`)
- `GET /bi/finance/overview?...` -> `200` com chave `aging`
- `GET /bi/dashboard/overview?...` -> `200` com chave `health_score`

Fase 5 (IA + custo):

- `POST /bi/jarvis/generate?dt_ref=2026-03-02&id_empresa=1&limit=5&force=true` -> `200`
  - `candidates=1`, `processed=1`, `fallback_used=1`, `cache_hits=0`
- `GET /bi/admin/ai-usage?days=90&id_empresa=1` -> `200`
  - `cache_rows=1`, `openai_calls=0`, `fallback_calls=1`

## Validação de STG shadow columns

Exemplo em `stg.produtos` após ingest:

- `id_db_shadow=9`
- `id_chave_natural=id_empresa=1|id_filial=1|id_produto=...`
- `dt_evento=2026-03-03 10:11:12+00`
- `received_at` preenchido

## Fase 4 (marts novas)

- `mart.customer_rfm_daily`: `1362` linhas
- `mart.customer_churn_risk_daily`: `1362` linhas
- `mart.finance_aging_daily`: `23` linhas
- `mart.health_score_daily`: `23` linhas

Refresh integrado executado com:

`etl.refresh_marts({'fact_venda':1,'fact_venda_item':1,'fact_comprovante':1,'risk_events':1,'fact_financeiro':1})`

Resultado:
- `sales_marts_refreshed=true`
- `churn_marts_refreshed=true`
- `finance_mart_refreshed=true`
- `finance_aging_refreshed=true`
- `risk_marts_refreshed=true`
- `health_score_refreshed=true`

## Testes e build

- Agent tests: `Ran 14 tests` -> `OK`
- API smoke tests (`make test`): `Ran 4 tests` -> `OK`
- Front build (`make lint`): Next.js build + lint/typecheck -> `OK`

## Amostra JSON Jarvis AI

Persistido em `app.insights_gerados.ai_plan`:

```json
{
  "priority": "HIGH",
  "diagnosis": "Margem atual 52.20% vs histórico 59.96%.",
  "actions_today": [
    "Revisar descontos e mix da loja no turno",
    "Investigar top 5 eventos por impacto e registrar acao corretiva"
  ],
  "expected_impact_range": "R$ 86,76 a R$ 216,89",
  "confidence": 0.55,
  "data_gaps": [
    "Fallback deterministico ativado: OPENAI_API_KEY not configured"
  ]
}
```

## Reteste OpenAI (chave configurada)

Execucao forcada com cache limpo:

- `POST /bi/jarvis/generate?dt_ref=2026-03-02&id_empresa=1&limit=5&force=true` -> `200`
  - `openai_calls=1`
  - `fallback_used=0`
  - `prompt_tokens=292`
  - `completion_tokens=235`
  - `estimated_cost_usd=0.0004928`
- `GET /bi/admin/ai-usage?days=90&id_empresa=1` -> `200`
  - `cache_rows=1`
  - `openai_calls=1`
  - `fallback_calls=0`

## Fase 6 (frontend premium) - gates

- `make lint` -> build/typecheck/lint do front `OK`
- `make test` -> `Ran 4 tests`, `OK`
- ETL tenant 1 (`etl.run_all(false,true)`):
  - 1a execucao: `67,244 ms`
  - 2a execucao: `49,081 ms`
- Endpoints:
  - `/bi/dashboard/overview` -> `200` (com `health_score`)
  - `/bi/clients/churn` -> `200`
  - `/bi/finance/overview` -> `200` (com `aging`)
- Contagens (tenant 1):
  - `stg.comprovantes`: `10,924,419`
  - `stg.movprodutos`: `10,909,620`
  - `stg.itensmovprodutos`: `18,546,845`
  - `dw.fact_comprovante`: `10,924,419`
  - `dw.fact_venda`: `10,909,620`
  - `dw.fact_venda_item`: `18,546,845`
  - `mart.agg_vendas_diaria`: `17,645`
  - `mart.agg_vendas_hora`: `335,407`

## Fase 7 (alertas in-app) - gates

- migration aplicada: `sql/migrations/011_phase7_notifications.sql`
- endpoints novos:
  - `GET /bi/notifications` -> `200`
  - `POST /bi/notifications/{id}/read` -> `200`
  - `GET /bi/notifications/unread-count` -> `200`
- prova de fluxo:
  - notificação CRITICAL criada (`list_items=1`)
  - `unread` antes: `1`
  - `mark_read` executado
  - `unread` depois: `0`
- ETL tenant 1 (`etl.run_all(false,true)`):
  - 1a execucao: `34,998 ms`
  - 2a execucao: `44,539 ms`
- contagens (tenant 1):
  - `stg.comprovantes`: `10,924,419`
  - `stg.movprodutos`: `10,909,620`
  - `stg.itensmovprodutos`: `18,546,845`
  - `dw.fact_comprovante`: `10,924,419`
  - `dw.fact_venda`: `10,909,620`
  - `dw.fact_venda_item`: `18,546,845`
  - `mart.agg_vendas_diaria`: `17,645`
  - `mart.agg_vendas_hora`: `335,407`
  - `app.notifications`: `1`

## Fase 8 (testes + CI + prova de nao quebra)

- smoke tests API ampliados:
  - ingest NDJSON -> STG
  - ETL incremental (2a execucao nao degrada)
  - `/bi/dashboard/overview`
  - `/bi/jarvis/generate` + `/bi/admin/ai-usage`
  - `/bi/notifications` + `unread-count` + `mark-read`
- resultado local API: `Ran 5 tests`, `OK`
- resultado local Agent: `Ran 14 tests`, `OK`
- build/lint/typecheck front: `OK`
- CI adicionada:
  - `.github/workflows/ci.yml`
  - pipeline: `docker compose up --build` -> seed -> `make test` -> `make test-agent` -> `make lint` -> teardown

## Validacao ponta a ponta final (2026-03-03)

Fluxo executado:

1. login OWNER
2. ingest NDJSON real (`/ingest/produtos`)
3. ETL tenant 1 duas vezes (`etl.run_all(1,false,true)`)
4. endpoints BI/Jarvis/alertas
5. contagens STG/DW/MART + app
6. gate unico `make ci`

Resultado objetivo:

- ingest:
  - `ingest_ok=true`
  - `inserted_or_updated=1`
- ETL:
  - `run1=50,130.82 ms`
  - `run2=47,267.40 ms`
- endpoints:
  - `/bi/dashboard/overview` -> `200` (com `health_score`)
  - `/bi/clients/churn` -> `200`
  - `/bi/finance/overview` -> `200`
  - `/bi/jarvis/generate` -> `200`
  - `/bi/admin/ai-usage` -> `200`
  - `/bi/notifications` -> `200`
  - `/bi/notifications/unread-count` -> `200`
- estado Jarvis IA:
  - `jarvis_generate.stats`: `processed=1`, `cache_hits=1`, `openai_calls=0`, `fallback_used=0`
  - `ai_usage.totals`: `cache_rows=2`, `openai_calls=1`, `fallback_calls=1`
- contagens finais tenant 1:
  - `stg.comprovantes`: `10,924,419`
  - `stg.movprodutos`: `10,909,620`
  - `stg.itensmovprodutos`: `18,546,845`
  - `dw.fact_comprovante`: `10,924,419`
  - `dw.fact_venda`: `10,909,620`
  - `dw.fact_venda_item`: `18,546,845`
  - `mart.agg_vendas_diaria`: `17,645`
  - `mart.agg_vendas_hora`: `335,407`
  - `mart.customer_churn_risk_daily`: `1,362`
  - `mart.finance_aging_daily`: `23`
  - `mart.health_score_daily`: `23`
  - `app.insight_ai_cache`: `2`
  - `app.notifications`: `1`
- gate final:
  - `make ci` -> `API smoke: Ran 5 tests OK` + `Agent: Ran 14 tests OK` + `Web build/typecheck OK`

## Observações

- `agent check` no host local falhou por ausência de `pyodbc` no ambiente da máquina (`python3`), não por erro de lógica do código.
- Foi feito bootstrap controlado de watermark por `received_at` nos datasets de venda/comprovante/itens após introdução da coluna (STG e DW já alinhados em contagem).
- Otimizações de Fase 3 aplicadas:
  - `sql/migrations/007_etl_incremental_hot_received.sql`
  - `sql/migrations/008_run_all_skip_risk_when_no_changes.sql`
- Diagnóstico de integração financeira em cadeia: `dw.fact_financeiro` está com `0` linhas no tenant 1 (ainda sem carga AR/AP), por isso:
  - `mart.finance_aging_daily.data_gaps = true`
  - health score aplica penalidade de dados faltantes em componente `dados`.
