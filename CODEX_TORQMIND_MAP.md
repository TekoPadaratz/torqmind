# CODEX TorqMind Map

Arquivo de contexto rapido para futuras sessoes. Leia este arquivo antes de reauditar a migracao analitica.

## 1. Arquitetura atual

- `apps/api`: FastAPI + Pydantic + JWT. Rotas BI em `app/routes_bi.py` e rotas historicas de dashboard em `app/routes_dashboard.py`.
- `apps/web`: Next.js 14 + TypeScript. Paginas BI consomem `/bi/*` via `apps/web/app/lib/api.ts`.
- PostgreSQL continua sendo fonte da verdade transacional e legado analitico (`stg`, `dw`, `mart`, `app`, `auth`, `billing`).
- ClickHouse replica o schema `dw` PostgreSQL no banco `torqmind_dw` via `MaterializedPostgreSQL`.
- ClickHouse serve marts nativas em `torqmind_mart` com tabelas agregadas/desnormalizadas e MVs streaming.
- O backend analitico usa `app.repos_analytics` como facade ClickHouse-first. As rotas continuam chamando nomes publicos iguais aos de `repos_mart`.
- Origem canonica de vendas: `stg.comprovantes` e `stg.itenscomprovantes`. `MovProdutos`/`ItensMovProdutos` nao devem voltar ao hot path de vendas; campos DW como `id_movprodutos` podem permanecer apenas como aliases legados preenchidos a partir de comprovantes.
- Timezone: infraestrutura segue em UTC; negocio e UI usam `America/Sao_Paulo`; filtros trafegam como `YYYY-MM-DD`; timestamps tecnicos da API devem sair ISO 8601 com offset explicito.

## 2. Rodar local com PostgreSQL + ClickHouse

Fluxo recomendado:

```bash
make setup
make up
make migrate
make clickhouse-init
make clickhouse-mvs
make clickhouse-native-backfill
make clickhouse-smoke
make analytics-smoke
make lint
make test
```

Atalhos uteis:

- `make up`: sobe Postgres, ClickHouse, API e Web.
- `make down`: para os servicos.
- `make logs`: segue logs.
- `make resetdb RESET_CONFIRM=1 RESET_ENV=dev`: reset destrutivo somente dev/homolog.
- `make clickhouse-dw-init`: cria `torqmind_dw` usando `MaterializedPostgreSQL`.
- `make clickhouse-marts-init`: cria tabelas `torqmind_mart`.
- `make clickhouse-mvs`: cria MVs streaming.
- `make clickhouse-native-backfill`: popula marts nativas a partir de `torqmind_dw`.
- `make analytics-smoke`: valida inventory do facade.
- `make prod-clickhouse-init`: em producao recria `torqmind_dw`, espera tabelas DW e espera `fact_venda`/`fact_venda_item` baterem count/max(data_key) com PostgreSQL antes de backfillar `torqmind_mart`.
- `make prod-data-reconcile ID_EMPRESA=1 ID_FILIAL=14458`: compara PostgreSQL DW, `torqmind_dw` e marts de vendas sem depender de `stg.movprodutos`.

## 3. Variaveis de ambiente criticas

- `USE_CLICKHOUSE=true|false`: ativa leitura analitica ClickHouse-first.
- `DUAL_READ_MODE=true|false`: executa Postgres + ClickHouse em paralelo quando possivel e loga divergencias.
- `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`: conexao ClickHouse.
- `CLICKHOUSE_DW_WAIT_ATTEMPTS`, `CLICKHOUSE_REPLICATION_WAIT_ATTEMPTS`: limites de espera do bootstrap ClickHouse de producao.
- `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`: conexao PostgreSQL.
- `DATABASE_URL`: URL async da API.
- `JWT_SECRET_KEY`/equivalentes em `config.py`: nunca logar nem commitar.
- `BUSINESS_TIMEZONE=America/Sao_Paulo`: fuso civil da regra de negocio.
- Em Docker local, a API usa `PG_PORT=5432` dentro da rede compose e ClickHouse em `clickhouse:8123`.

## 4. Mapa de arquivos principais

Backend:

- `apps/api/app/config.py`: settings, incluindo `use_clickhouse` e `dual_read_mode`.
- `apps/api/app/db.py`: pool PostgreSQL.
- `apps/api/app/db_clickhouse.py`: cliente ClickHouse, `query_dict`, `query_scalar`, batch insert e validadores dual-read.
- `apps/api/app/repos_mart.py`: repositorio legado PostgreSQL.
- `apps/api/app/repos_mart_clickhouse.py`: implementacao ClickHouse Smart Marts.
- `apps/api/app/repos_analytics.py`: facade de selecao ClickHouse/Postgres.
- `apps/api/app/routes_bi.py`: endpoints `/bi/*`.
- `apps/api/app/routes_dashboard.py`: endpoints `/dashboard/*`.
- `apps/api/app/scope.py`: escopo tenant/filial.
- `apps/api/app/services/snapshot_cache.py`: cache de snapshots operacionais.

Frontend:

- `apps/web/app/lib/api.ts`: cliente HTTP e contratos consumidos pelas paginas.
- `apps/web/app/dashboard/page.tsx`
- `apps/web/app/sales/page.tsx`
- `apps/web/app/fraud/page.tsx`
- `apps/web/app/customers/page.tsx`
- `apps/web/app/finance/page.tsx`
- `apps/web/app/goals/page.tsx`
- `apps/web/app/cash/page.tsx`
- `apps/web/app/pricing/page.tsx`
- `apps/web/app/scope/page.tsx`

SQL PostgreSQL:

- `sql/migrations/*.sql`: cadeia versionada.
- `sql/torqmind_reset_db_v2.sql`: reset dev/homolog alinhado ate a migration `071`.

SQL ClickHouse:

- `sql/clickhouse/phase2_postgres_to_clickhouse_mapping.md`: mapeamento DW -> CH.
- `sql/clickhouse/phase2_mvs_design.sql`: tabelas marts.
- `sql/clickhouse/phase2_mvs_streaming_triggers.sql`: MVs streaming.
- `sql/clickhouse/phase3_native_backfill.sql`: backfill nativo.

Deploy:

- `docker-compose.yml`: stack local com API/Web/Postgres/ClickHouse.
- `docker-compose.clickhouse.yml`: servico ClickHouse local.
- `docker-compose.prod.yml`: stack prod com ClickHouse.
- `.env.production.example`: variaveis prod esperadas.
- `deploy/scripts/load_clickhouse_historical.sh`: carga historica CH.
- `deploy/scripts/prod-clickhouse-init.sh`: bootstrap prod; nao backfilla marts antes de `torqmind_dw.fact_venda` e `fact_venda_item` atingirem count/max(data_key) do PostgreSQL.
- `deploy/scripts/prod-data-reconcile.sh`: reconciliacao DW PostgreSQL vs ClickHouse DW vs marts.
- `Makefile`: fonte unica dos comandos operacionais.

Testes:

- `apps/api/app/test_db_clickhouse_unit.py`: unidade do cliente ClickHouse.
- `apps/api/app/test_repos_analytics_unit.py`: facade e escopo.
- `apps/api/app/test_routes_bi_branch_scope_unit.py`: contratos de rotas BI/filial.
- Demais `apps/api/app/test_*.py`: regressao de auth, ETL, operacional, release hardening.

## 5. Mapa das funcoes analiticas

Facade:

- Rotas importam `from app import repos_analytics as repos_mart`.
- `USE_CLICKHOUSE=true`: usa `repos_mart_clickhouse` quando a funcao existe.
- `USE_CLICKHOUSE=false`: usa `repos_mart` legado.
- Erro ClickHouse com `USE_CLICKHOUSE=true` propaga e aparece em log; nao ha zero silencioso.
- `DUAL_READ_MODE=true`: executa ambos quando viavel e loga divergencias via validator.

Funcoes ClickHouse implementadas:

| Funcao | Endpoints principais | Fonte CH | Contrato |
|---|---|---|---|
| `dashboard_kpis` | `/bi/dashboard/overview`, `/dashboard/kpis` | `agg_vendas_diaria` | KPIs gerais |
| `dashboard_series` | `/bi/dashboard/overview`, `/dashboard/series` | `agg_vendas_diaria` | serie diaria |
| `dashboard_home_bundle` | `/bi/dashboard/home` | varias marts | bundle home |
| `insights_base` | `/dashboard/insights` | `insights_base_diaria` | insights base |
| `commercial_window_coverage` | sales/dashboard | `agg_vendas_diaria` | cobertura janela |
| `sales_by_hour` | `/bi/sales/overview` | `agg_vendas_hora` | serie por hora |
| `sales_top_products` | `/bi/sales/overview` | `agg_produtos_diaria` | ranking produtos |
| `sales_top_groups` | `/bi/sales/overview` | `agg_grupos_diaria` | ranking grupos |
| `sales_top_employees` | `/bi/sales/overview`, goals | `agg_funcionarios_diaria` | ranking funcionarios |
| `sales_commercial_overview` | `/bi/sales/overview` | vendas/produtos/grupos/funcionarios | objeto comercial |
| `sales_overview_bundle` | `/bi/sales/overview` | vendas + rankings | payload sales |
| `_sales_sync_meta` | home/sales/cash | `agg_vendas_diaria.updated_at` | publicacao tecnica ISO com offset |
| `sales_operational_current` | sales/cash | `agg_vendas_diaria` | atual operacional |
| `sales_operational_day_bundle` | sales/cash | vendas mart | dia operacional |
| `sales_operational_range_bundle` | sales/cash | vendas mart | periodo operacional |
| `fraud_kpis` | `/bi/fraud/overview` | `fraude_cancelamentos_diaria` | KPIs fraude |
| `fraud_series` | `/bi/fraud/overview` | `fraude_cancelamentos_diaria` | serie fraude |
| `fraud_data_window` | `/bi/fraud/overview` | `fraude_cancelamentos_diaria` | janela dados |
| `fraud_last_events` | `/bi/fraud/overview` | `fraude_cancelamentos_eventos` | eventos recentes |
| `fraud_top_users` | `/bi/fraud/overview` | `fraude_cancelamentos_eventos` | usuarios top |
| `risk_kpis` | `/bi/risk/overview` | `agg_risco_diaria` | KPIs risco |
| `risk_series` | `/bi/risk/overview` | `agg_risco_diaria` | serie risco |
| `risk_data_window` | `/bi/risk/overview` | `agg_risco_diaria` | janela dados |
| `risk_top_employees` | `/bi/risk/overview` | `risco_top_funcionarios_diaria` | ranking risco |
| `risk_last_events` | `/bi/risk/overview` | `risco_eventos_recentes` | eventos risco |
| `risk_by_turn_local` | `/bi/risk/overview` | `risco_turno_local_diaria` | risco turno/local |
| `operational_score` | `/bi/risk/overview` | risco + vendas + caixa | score operacional |
| `customers_top` | `/bi/customers/overview` | `customer_rfm_daily` | top clientes |
| `customers_rfm_snapshot` | `/bi/customers/overview` | `customer_rfm_daily` | snapshot RFM |
| `customers_churn_risk` | `/bi/clients/churn` | `customer_churn_risk_daily` | risco churn |
| `customers_churn_bundle` | `/bi/clients/churn` | churn + RFM | bundle churn |
| `customers_churn_diamond` | `/bi/clients/churn` | `clientes_churn_risco` | diamante churn |
| `customers_churn_snapshot_meta` | `/bi/clients/churn` | churn marts | metadados |
| `customer_churn_drilldown` | `/bi/clients/churn` | `customer_churn_risk_daily` | drilldown |
| `anonymous_retention_overview` | `/bi/clients/retention-anonymous` | `anonymous_retention_daily` | retencao anonima |
| `finance_kpis` | `/bi/finance/overview` | `financeiro_vencimentos_diaria` | KPIs financeiro |
| `finance_series` | `/bi/finance/overview` | `financeiro_vencimentos_diaria` | serie financeiro |
| `finance_aging_overview` | `/bi/finance/overview` | `finance_aging_daily` | aging |
| `payments_overview_kpis` | `/bi/payments/overview` | `agg_pagamentos_diaria` | KPIs pagamentos |
| `payments_by_day` | `/bi/payments/overview` | `agg_pagamentos_diaria` | serie pagamentos |
| `payments_by_turno` | `/bi/payments/overview` | `agg_pagamentos_turno` | turno pagamentos |
| `payments_anomalies` | `/bi/payments/overview` | `pagamentos_anomalias_diaria` | anomalias |
| `payments_overview` | `/bi/payments/overview` | pagamentos marts | payload pagamentos |
| `cash_commercial_overview` | `/bi/cash/overview` | vendas + caixa | comercial caixa |
| `cash_overview` | `/bi/cash/overview` | caixa marts | payload caixa |
| `open_cash_monitor` | `/bi/cash/overview` | `alerta_caixa_aberto` | alertas caixa |
| `health_score_latest` | dashboard/home | `health_score_daily` | score saude |
| `leaderboard_employees` | `/bi/goals/overview` | `agg_funcionarios_diaria` | ranking metas |
| `sales_peak_hours_signal` | Jarvis/insights | `agg_vendas_hora` | sinal horarios |
| `sales_declining_products_signal` | Jarvis/insights | `agg_produtos_diaria` | sinal produtos |
| `jarvis_briefing` | `/bi/jarvis/briefing` | marts analiticas | briefing |

Funcoes estaticas/definicoes tambem existem em CH: `cash_definitions`, `fraud_definitions`, `finance_definitions`, `risk_model_coverage`.

Funcoes Postgres por desenho:

- `list_filiais`: auth/app scope.
- `competitor_pricing_upsert`: escrita OLTP app.
- `competitor_fuel_product_ids`: dimensao/app para formulario.
- `goals_today`, `upsert_goal`: app goals.
- `risk_insights`: app/insights operacionais.
- `notifications_list`, `notifications_unread_count`, `notification_mark_read`: app notifications.

Divida tecnica explicita quando `USE_CLICKHOUSE=true`:

- `stock_position_summary`: falta mart de estoque.
- `customers_delinquency_overview`: falta mart customer-level de inadimplencia.
- `cash_dre_summary`: DRE ainda depende de fatos financeiros transacionais.
- `competitor_pricing_overview`: mistura app competitor table + dimensao combustivel.
- `monthly_goal_projection`: mistura `app.goals` com serie analitica.

## 6. Mapa das marts ClickHouse

| Tabela | Finalidade | Colunas-chave | Consumidores |
|---|---|---|---|
| `agg_vendas_diaria` | vendas diarias | `id_empresa`, `id_filial`, `data_key`, `faturamento`, `margem`, `vendas`, `ticket_medio` | dashboard, sales, goals |
| `agg_vendas_hora` | vendas por hora | `id_empresa`, `id_filial`, `data_key`, `hora`, `faturamento`, `vendas` | sales, Jarvis |
| `agg_produtos_diaria` | produtos agregados | `id_produto`, `produto_nome`, `faturamento`, `qtd`, `custo_total`, `margem` | sales, Jarvis |
| `agg_grupos_diaria` | grupos agregados | `id_grupo_produto`, `grupo_nome`, `faturamento`, `margem` | sales |
| `agg_funcionarios_diaria` | funcionarios agregados | `id_funcionario`, `funcionario_nome`, `faturamento`, `margem`, `vendas` | sales, goals |
| `insights_base_diaria` | indicadores base | `data_key`, `faturamento`, `margem`, `risco_score` | insights |
| `fraude_cancelamentos_diaria` | fraude cancelamentos | `cancelamentos`, `valor_cancelado`, `usuarios_distintos` | fraud |
| `fraude_cancelamentos_eventos` | eventos fraude | `id_movprodutos`, `usuario_nome`, `motivo`, `valor` | fraud |
| `agg_risco_diaria` | risco diario | `eventos`, `valor_risco`, `risco_score` | risk |
| `risco_top_funcionarios_diaria` | risco por funcionario | `id_funcionario`, `funcionario_nome`, `eventos`, `valor_risco` | risk |
| `risco_turno_local_diaria` | risco por turno/local | `id_turno`, `id_local_venda`, `eventos`, `valor_risco` | risk |
| `risco_eventos_recentes` | view eventos recentes | `id`, `id_movprodutos`, `funcionario_nome`, `severity`, `score` | risk |
| `clientes_churn_risco` | churn legado | `id_cliente`, `cliente_nome`, `score_churn` | customers |
| `customer_rfm_daily` | RFM cliente | `id_cliente`, `recency_days`, `frequency`, `monetary` | customers |
| `customer_churn_risk_daily` | churn diario | `id_cliente`, `risk_score`, `risk_bucket` | churn |
| `financeiro_vencimentos_diaria` | vencimentos | `vencido`, `a_vencer`, `recebido`, `pago` | finance |
| `finance_aging_daily` | aging financeiro | `bucket`, `valor`, `quantidade` | finance |
| `agg_pagamentos_diaria` | pagamentos diarios | `forma_pagamento`, `total_valor`, `qtd` | payments |
| `agg_pagamentos_turno` | pagamentos por turno | `id_turno`, `forma_pagamento`, `total_valor` | payments |
| `pagamentos_anomalias_diaria` | anomalias pagamento | `event_type`, `severity`, `score`, `insight_id_hash` | payments, notifications sync |
| `agg_caixa_turno_aberto` | caixa aberto | `id_turno`, `opened_at`, `expected_total`, `observed_total` | cash |
| `agg_caixa_forma_pagamento` | caixa por forma | `forma_pagamento`, `valor` | cash |
| `agg_caixa_cancelamentos` | cancelamentos caixa | `valor_cancelado`, `qtd_cancelamentos` | cash |
| `alerta_caixa_aberto` | alertas caixa | `status`, `hours_open`, `severity` | cash |
| `anonymous_retention_daily` | retencao anonima | `new_customers`, `returning_customers`, `retention_rate` | retention |
| `health_score_daily` | health score | `final_score`, `sales_pct`, `risk_pct`, `cash_pct`, `customer_pct` | home/dashboard |

## 7. Decisoes de arquitetura tomadas

- Criado `repos_analytics.py` em vez de transformar `repos_mart.py`, preservando o legado inteiro e reduzindo risco de breaking change.
- Rotas BI e dashboard importam o facade como `repos_mart`, mantendo o restante do codigo estavel.
- `USE_CLICKHOUSE=true` nao mascara falhas ClickHouse: excecao propaga.
- Funcoes sem mart equivalente sao divida explicita no facade e geram warning quando usadas com ClickHouse ativo.
- `query_dict()` agora retorna `list[dict]` real usando `column_names`, pois as funcoes usam `row.get(...)`.
- Docker local/prod tem ClickHouse como servico de primeira classe.
- `MaterializedPostgreSQL` usa banco `torqmind_dw` replicando schema `dw`; marts vivem separadas em `torqmind_mart`.
- Frescor operacional ClickHouse separa cobertura comercial (`commercial_coverage.latest_available_dt`), publicacao tecnica (`operational_sync.last_sync_at`) e frescor de tela (`freshness.live_through_at`).
- `prod-clickhouse-init.sh` deve esperar count/max(data_key) de `torqmind_dw.fact_venda` e `torqmind_dw.fact_venda_item` baterem com PostgreSQL antes de criar/backfillar marts.
- Frontend nunca deve montar filtro BI com `toISOString().slice(0, 10)`; datas de negocio ficam como string `YYYY-MM-DD`.

## 8. Pontas soltas resolvidas

- Rotas nao ficam mais presas acidentalmente ao repositorio PostgreSQL legado.
- Cliente ClickHouse corrigido para timeouts aceitos por `clickhouse-connect`.
- `query_dict()` corrigido para linhas dict, com teste unitario.
- Marts de produto passaram a carregar `custo_total`.
- `risco_eventos_recentes` passou a expor campos exigidos pelo backend.
- Compose local/prod agora inclui ClickHouse e variaveis da API.
- Makefile ganhou init/backfill/smoke ClickHouse.
- Reset SQL alinhado ate a migration `070`.
- Reparados constraints/indices necessarios para upserts e para `MaterializedPostgreSQL`/replica identity.
- Reparada sincronizacao de notificacoes de anomalia de pagamento.
- Sanitizado float nao finito vindo de agregacoes ClickHouse antes de montar JSON.
- Corrigido frescor de vendas para usar `agg_vendas_diaria.updated_at` com conversao UTC -> `America/Sao_Paulo`.
- Corrigido caixa para nao devolver `1970-01-01T00:00:00` quando nao ha linha util em `agg_caixa_turno_aberto`.
- Corrigido frontend para nao fixar sync indisponivel quando existe cobertura comercial publicada.
- Corrigido bootstrap ClickHouse prod para aguardar replicacao completa de vendas antes do backfill das marts.
- Adicionado script de reconciliacao `prod-data-reconcile.sh`.

## 9. Pontas soltas remanescentes

- Ainda nao existe mart ClickHouse para estoque, DRE, inadimplencia por cliente, precificacao concorrente e projecao mensal de metas.
- Alguns testes legados criam dados diretamente no PostgreSQL e esperam leitura imediata em endpoints BI; com `USE_CLICKHOUSE=true`, esses testes precisam popular ClickHouse ou rodar com `USE_CLICKHOUSE=false`.
- O `make test` completo ainda nao esta verde: restam falhas de ETL/fixtures legadas e smokes que batem no servidor externo com estado diferente do processo de teste.
- Cobertura e2e Docker com autenticacao real dos endpoints `/bi/*` ainda deve ser consolidada com dados seedados representativos.
- Queries ClickHouse foram mantidas sem JOIN pesado no hot path, mas o tuning de ORDER BY/TTL/projecoes deve ser revisto com cardinalidade real de producao.

## 10. Checklist para futuras alteracoes

- Preservar assinatura publica das funcoes de repositorio.
- Preservar formato JSON consumido por `apps/web/app/lib/api.ts` e paginas.
- Toda query analitica precisa filtrar `id_empresa`.
- Usar `id_filial` quando o endpoint aceitar escopo de filial; `None` e `-1` significam todas.
- Preferir `torqmind_mart` antes de `torqmind_dw`.
- Se faltar mart, criar SQL/backfill/MV ou registrar divida no facade.
- Nao retornar zero silencioso em erro de infraestrutura ClickHouse.
- Atualizar `phase2_mvs_design.sql`, `phase2_mvs_streaming_triggers.sql` e `phase3_native_backfill.sql` juntos quando a mart mudar.
- Atualizar `CODEX_TORQMIND_MAP.md` quando adicionar/remover funcao ou mart.
- Rodar pelo menos testes unitarios do facade/ClickHouse, `make lint`, `make clickhouse-smoke` e `make analytics-smoke`.
- Ao mexer em vendas, confirmar que ETL ativo usa `stg.comprovantes`/`stg.itenscomprovantes`, nao `stg.movprodutos`.
- Ao mexer em datas no frontend, adicionar teste cobrindo horario noturno em `America/Sao_Paulo`.

## 11. Comandos validados nesta revisao

- `make migrate`: passou, aplicando migrations novas ate `070`.
- `docker compose up -d --build`: passou.
- `docker compose up -d --build api`: passou.
- `curl -fsS http://localhost:8123/ping`: retornou `Ok.`
- `curl -fsS http://localhost:18000/health`: retornou health OK.
- `make clickhouse-wait-dw`: passou.
- `make clickhouse-marts-init`: passou.
- `make clickhouse-mvs`: passou.
- `make clickhouse-native-backfill`: passou.
- `make clickhouse-smoke`: passou com `torqmind_dw` e `torqmind_mart`.
- `make analytics-smoke`: passou e mostrou 68 funcoes no facade.
- `docker compose exec -T api python -m unittest app.test_db_clickhouse_unit app.test_repos_analytics_unit app.test_routes_bi_branch_scope_unit`: passou, 18 testes.
- `docker compose exec -T web npm test`: passou, 69 testes.
- `make lint`: passou.
- `make test`: executado; falhou na API com falhas remanescentes documentadas nesta secao.
- Revisao 2026-04-29: `cd apps/api && python -m unittest app.test_repos_analytics_unit` passou, 14 testes.
- Revisao 2026-04-29: `cd apps/web && npm test` passou, 74 testes.
- Revisao 2026-04-29: `docker compose exec -T api python -m unittest app.test_repos_analytics_unit` passou, 14 testes.
- Revisao 2026-04-29: `docker compose exec -T web npm test` passou, 74 testes.
- Revisao 2026-04-29: `make migrate`, `make clickhouse-init`, `make clickhouse-mvs`, `make clickhouse-native-backfill`, `make clickhouse-smoke`, `make analytics-smoke` e `make lint` passaram no compose local.
- Revisao 2026-04-29: `docker compose -f docker-compose.prod.yml --env-file .env.production.example config --quiet` passou.
- Revisao 2026-04-29: chamada direta de `dashboard_home_bundle` retornou `operational_sync.last_sync_at` com offset `-03:00`, cobertura `exact` e caixa sem `1970-01-01T00:00:00`.
- Revisao 2026-04-29: `prod-data-reconcile.sh` rodou contra compose local com `ID_EMPRESA=1 ID_FILIAL=14458`; expôs `83` itens órfãos em `dw.fact_venda_item`, mas PostgreSQL DW e `torqmind_dw` bateram count/max(data_key).
- Revisao 2026-04-29: `bash -n deploy/scripts/prod-clickhouse-init.sh deploy/scripts/prod-data-reconcile.sh` passou.
- Revisao 2026-04-29: funcoes ativas `etl.load_fact_venda` e `etl.load_fact_venda_item_range_detail` confirmadas em PostgreSQL usando `stg.comprovantes` e `stg.itenscomprovantes`.
- Revisao 2026-04-29: `make test` foi tentado; testes API legados apresentaram multiplas falhas/erros de fixtures/estado e foi interrompido apos ficar sem progresso, retornando `Error 130`.

## 12. Regras de ouro

- Nunca remover filtro `id_empresa`.
- Nunca quebrar JSON do frontend.
- Preferir `torqmind_mart`.
- Nao mascarar erro critico com zero silencioso.
- Manter fallback explicito via `USE_CLICKHOUSE=false`.
- Atualizar testes junto com contrato, SQL e facade.
- Nunca reintroduzir `stg.movprodutos` como fonte canonica de vendas.
- Infra UTC; negocio/UI `America/Sao_Paulo`; filtros sempre `YYYY-MM-DD`; API retorna timestamp tecnico com offset.
- Bootstrap ClickHouse: esperar DW replicado antes de backfillar marts.
