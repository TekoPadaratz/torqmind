# CODEX TorqMind Map

Arquivo de contexto rapido para futuras sessoes. Leia este arquivo antes de reauditar a migracao analitica.

## 1. Arquitetura atual

- `apps/api`: FastAPI + Pydantic + JWT. Rotas BI em `app/routes_bi.py` e rotas historicas de dashboard em `app/routes_dashboard.py`.
- `apps/web`: Next.js 14 + TypeScript. Paginas BI consomem `/bi/*` via `apps/web/app/lib/api.ts`.
- PostgreSQL continua sendo fonte da verdade transacional e legado analitico (`stg`, `dw`, `mart`, `app`, `auth`, `billing`).
- ClickHouse mantem uma copia analitica nativa do schema `dw` PostgreSQL no banco `torqmind_dw`, carregada explicitamente por script via table function `postgresql(...)`.
- ClickHouse serve marts nativas em `torqmind_mart` com tabelas agregadas/desnormalizadas e MVs streaming.
- Operacao normal: ETL incremental atualiza PostgreSQL DW, depois `prod-clickhouse-sync-dw.sh MODE=incremental` publica `torqmind_dw` nativo e `prod-clickhouse-refresh-marts.sh MODE=incremental` republica janelas afetadas nas marts.
- O backend analitico usa `app.repos_analytics` como facade ClickHouse-first. As rotas continuam chamando nomes publicos iguais aos de `repos_mart`.
- Origem canonica de vendas: `stg.comprovantes` e `stg.itenscomprovantes`. `MovProdutos`/`ItensMovProdutos` nao devem voltar ao hot path de vendas; campos DW como `id_movprodutos` podem permanecer apenas como aliases legados preenchidos a partir de comprovantes.
- Timezone: infraestrutura segue em UTC; negocio e UI usam `America/Sao_Paulo`; filtros trafegam como `YYYY-MM-DD`; timestamps tecnicos da API devem sair ISO 8601 com offset explicito.

## 2. Rodar local com PostgreSQL + ClickHouse

Fluxo recomendado:

```bash
make setup
make up
make migrate
make clickhouse-sync-dw
make clickhouse-marts-init
make clickhouse-native-backfill
make clickhouse-mvs
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
- `make clickhouse-sync-dw`: recria `torqmind_dw` como banco ClickHouse nativo e carrega `dw.*` do PostgreSQL via `postgresql(...)`.
- `make clickhouse-dw-init`: alias local para `make clickhouse-sync-dw`.
- `make clickhouse-marts-init`: cria tabelas `torqmind_mart`.
- `make clickhouse-mvs`: cria MVs streaming.
- `make clickhouse-native-backfill`: popula marts nativas a partir de `torqmind_dw`.
- `make analytics-smoke`: valida inventory do facade.
- `make clickhouse-init`: executa sync DW nativo, espera as 14 tabelas obrigatorias e cria tabelas mart; para refresh completo rode backfill e MVs em seguida.
- `make prod-clickhouse-sync-dw`: sync produtivo PostgreSQL DW -> ClickHouse DW nativo.
- `make prod-clickhouse-sync-dw-full`: full refresh controlado do DW ClickHouse nativo.
- `make prod-clickhouse-sync-dw-incremental`: sync incremental do DW ClickHouse nativo.
- `make prod-clickhouse-refresh-marts-full`: recria/repopula marts.
- `make prod-clickhouse-refresh-marts-incremental`: republica somente janela afetada.
- `make prod-history-coverage-audit ID_EMPRESA=1 ID_FILIAL=14458`: audita cobertura historica sem usar `MovProdutos`.
- `make prod-sales-orphans-report ID_EMPRESA=1 ID_FILIAL=14458`: relata itens orfaos como WARN, sem apagar dados.
- `make prod-clickhouse-init`: em producao recria `torqmind_dw` nativo, valida `fact_venda`/`fact_venda_item` contra PostgreSQL, recria `torqmind_mart`, roda backfill e cria MVs streaming nesta ordem.
- `make prod-data-reconcile ID_EMPRESA=1 ID_FILIAL=14458`: compara PostgreSQL DW, `torqmind_dw` e marts de vendas sem depender de `stg.movprodutos`.
- `make prod-semantic-marts-audit ID_EMPRESA=1 ID_FILIAL=14458`: valida semantica de labels humanos em pagamentos, caixa, antifraude, risco, financeiro e concorrencia.
- `make prod-homologation-apply`: orquestrador unico para homologacao com preflight, pause/resume de cron, build/recreate, migrate, ClickHouse full, audits e post-boot checks.
- `make prod-homologation-apply-streaming`: mesmo fluxo, mas com bootstrap e validacao do streaming 2.0 em paralelo.
- `make prod-homologation-apply-full-stg`: rebuild completo desde STG (todas as filiais) com ClickHouse full, desde 2025-01-01.
- `make prod-rebuild-derived-from-stg FROM_DATE=2025-01-01 ID_EMPRESA=1`: rebuild seguro das camadas derivadas PostgreSQL desde a STG, sem tocar ClickHouse.
- `make prod-rebuild-derived-from-stg FROM_DATE=2025-01-01 ID_EMPRESA=1 INCLUDE_DIMENSIONS=1`: mesma rotina, mas incluindo purge de dimensoes DW reconstruiveis em rebuild tenant-wide aberto.

Nota sobre filiais no apply:
- `--id-filial` = escopo de auditoria (default 14458). Nao afeta o rebuild.
- `--rebuild-id-filial` = escopo do rebuild derivado. Omitir = todas as filiais.
- `--all-filiais` = alias explícito para rebuild de todas as filiais.

## 3. Variaveis de ambiente criticas

- `USE_CLICKHOUSE=true|false`: ativa leitura analitica ClickHouse-first.
- `DUAL_READ_MODE=true|false`: executa Postgres + ClickHouse em paralelo quando possivel e loga divergencias.
- `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`: conexao ClickHouse.
- `CLICKHOUSE_PG_HOST`, `CLICKHOUSE_PG_PORT`: host/porta PostgreSQL acessiveis pelo ClickHouse para a table function `postgresql(...)`.
- `CLICKHOUSE_DW_WAIT_ATTEMPTS`: limite de espera local por tabelas DW nativas.
- `REFRESH_LEGACY_PG_MARTS=false`: desativa refresh global de marts PostgreSQL legadas no ETL; o hot path BI usa ClickHouse.
- `OPERATIONAL_INTERVAL_MINUTES=2`: intervalo recomendado do cron leve, com lock anti-overlap.
- `RISK_INTERVAL_MINUTES=30`: intervalo default do trilho risk.
- `PIPELINE_TIMEOUT_SECONDS`, `PIPELINE_WARN_SECONDS`, `PIPELINE_TRACK_LOG_MAX_BYTES`: limites de tempo/log do ciclo operacional.
- `CLICKHOUSE_INCREMENTAL_ENABLED=true`: habilita sync DW + refresh marts apos tracks com mudancas.
- `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`: conexao PostgreSQL.
- `DATABASE_URL`: URL async da API.
- `JWT_SECRET_KEY`/equivalentes em `config.py`: nunca logar nem commitar.
- `BUSINESS_TIMEZONE=America/Sao_Paulo`: fuso civil da regra de negocio.
- Em Docker local, a API usa `PG_PORT=5432` dentro da rede compose e ClickHouse em `clickhouse:8123`.

## 4. Mapa de arquivos principais

Backend:

- `apps/api/app/config.py`: settings, incluindo `use_clickhouse` e `dual_read_mode`.
- `apps/api/app/db.py`: pool PostgreSQL.
- `apps/api/app/db_clickhouse.py`: cliente ClickHouse por contexto/query, `query_dict`, `query_scalar`, batch insert e validadores dual-read.
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
- `sql/migrations/072_derived_rebuild_runtime_scope.sql`: helpers de runtime (`etl.from_date`, `etl.to_date`, `etl.branch_id`, `etl.force_full_scan`) e redefs das funcoes ETL ativas para rebuild derivado controlado, incluindo os wrappers publicos `load_fact_pagamento_comprovante*` e `load_fact_venda_item*`.

SQL ClickHouse:

- `sql/clickhouse/phase2_postgres_to_clickhouse_mapping.md`: mapeamento DW -> CH.
- `sql/clickhouse/phase2_mvs_design.sql`: tabelas marts.
- `sql/clickhouse/phase2_mvs_streaming_triggers.sql`: MVs streaming.
- `sql/clickhouse/phase3_native_backfill.sql`: backfill nativo; configura `max_partitions_per_insert_block=0` somente na sessao de backfill historico.

Deploy:

- `docker-compose.yml`: stack local com API/Web/Postgres/ClickHouse.
- `docker-compose.clickhouse.yml`: servico ClickHouse local.
- `docker-compose.prod.yml`: stack prod com ClickHouse.
- `.env.production.example`: variaveis prod esperadas.
- `deploy/scripts/load_clickhouse_historical.sh`: carga historica CH.
- `deploy/scripts/prod-clickhouse-sync-dw.sh`: cria `torqmind_dw` nativo, carrega as 14 tabelas DW obrigatorias por `postgresql(...)`, carrega `app.payment_type_map` em `dim_forma_pagamento` e valida counts/max(data_key); nao imprime credenciais.
- `deploy/scripts/prod-clickhouse-refresh-marts.sh`: modo `full` para bootstrap e `incremental` para republicar janelas afetadas de marts idempotentes.
- `deploy/scripts/prod-clickhouse-init.sh`: bootstrap prod; executa sync DW nativo, valida vendas, recria marts, roda backfill e depois cria MVs streaming.
- `deploy/scripts/prod-data-reconcile.sh`: reconciliacao DW PostgreSQL vs ClickHouse DW nativo vs marts; diferencia ERROR critico de WARN de qualidade.
- `deploy/scripts/prod-semantic-marts-audit.sh`: auditoria semantica das marts; falha em `FORMA_*`, labels vazios quando ha dimensao, datas 1970 e finance mart ausente com fatos existentes.
- `deploy/scripts/prod-etl-pipeline.sh`: rotina leve com lock, timeout, ETL operational/risk e publicacao incremental ClickHouse.
- `deploy/scripts/prod-install-cron.sh`: instala cron `*/${OPERATIONAL_INTERVAL_MINUTES}`; default operacional 2 min e risk 30 min.
- `deploy/scripts/prod-history-coverage-audit.sh`: auditoria historica por STG canonico, DW PostgreSQL, DW ClickHouse e mart.
- `deploy/scripts/prod-rebuild-derived-from-stg.sh`: audita cobertura STG, purga somente fatos derivados seguros, opcionalmente inclui dimensoes DW reconstruiveis com `--include-dimensions`, roda ETL full canônico com janela controlada e `force_full_scan`, e verifica STG vs DW sem tocar ClickHouse.
- `deploy/scripts/prod-sales-orphans-report.sh`: relatorio de orfaos de venda; nao deleta nada.
- `deploy/scripts/prod-homologation-apply.sh`: apply unico seguro para homolog/prod; valida env e compose, pausa cron, rebuilda API/Web/Nginx, roda migrate, opcionalmente faz rebuild derivado desde a STG (com `--rebuild-id-filial` para escopo ou todas as filiais por default), aceita `--include-dimensions` e o escape hatch explicito `--allow-dw-only --skip-clickhouse`, executa ClickHouse full ou incremental, audits, streaming opcional, limpeza de `app.snapshot_cache`, post-boot checks e resume cron sem `down -v` nem apagar volumes.
- `Makefile`: fonte unica dos comandos operacionais.

Runbook do apply unico:

- `docs/HOMOLOGATION_APPLY_RUNBOOK.md`: explica quando usar `--full-clickhouse`, `--with-streaming` e `--streaming-non-blocking`, como acompanhar logs e como fazer rollback basico sem tocar em volumes.
- `docs/DERIVED_REBUILD_FROM_STG_RUNBOOK.md`: explica quando usar o rebuild derivado puro, a diferenca entre STG -> DW e DW -> ClickHouse, e a semantica segura de `force_full_scan` em rebuild tenant-wide vs rebuild escopado.

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
| `sales_overview_bundle` | `/bi/sales/overview` | vendas + rankings | payload sales; usa mart publicada tambem para janela com dia atual |
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
| `cash_dre_summary` | `/bi/cash/overview` | `finance_aging_daily` | DRE/resumo financeiro sem datas 1970 |
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
- `competitor_pricing_overview`: fluxo app de precificacao concorrente; le valores salvos em PostgreSQL e bypassa snapshot.
- `competitor_pricing_upsert`: escrita OLTP app.
- `competitor_fuel_product_ids`: dimensao/app para formulario.
- `goals_today`, `upsert_goal`: app goals.
- `risk_insights`: app/insights operacionais.
- `notifications_list`, `notifications_unread_count`, `notification_mark_read`: app notifications.

Divida tecnica explicita quando `USE_CLICKHOUSE=true`:

- `stock_position_summary`: falta mart de estoque.
- `customers_delinquency_overview`: falta mart customer-level de inadimplencia.
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
| `fraude_cancelamentos_eventos` | eventos fraude | `id_filial`, `filial_nome`, `id_usuario`, `usuario_nome`, `usuario_source`, `id_turno`, `turno_value`, `valor_total` | fraud |
| `agg_risco_diaria` | risco diario | `eventos`, `valor_risco`, `risco_score` | risk |
| `risco_top_funcionarios_diaria` | risco por funcionario | `id_funcionario`, `funcionario_nome`, `eventos`, `valor_risco` | risk |
| `risco_turno_local_diaria` | risco por turno/local | `id_turno`, `turno_value`, `id_local_venda`, `local_nome`, `filial_nome`, `eventos`, `score_medio` | risk |
| `risco_eventos_recentes` | view eventos recentes | `id`, `id_movprodutos`, `filial_nome`, `funcionario_nome`, `operador_caixa_nome`, `event_type`, `score_risco` | risk |
| `clientes_churn_risco` | churn legado | `id_cliente`, `cliente_nome`, `score_churn` | customers |
| `customer_rfm_daily` | RFM cliente | `id_cliente`, `recency_days`, `frequency`, `monetary` | customers |
| `customer_churn_risk_daily` | churn diario | `id_cliente`, `risk_score`, `risk_bucket` | churn |
| `financeiro_vencimentos_diaria` | vencimentos | `vencido`, `a_vencer`, `recebido`, `pago` | finance |
| `finance_aging_daily` | aging financeiro | `bucket`, `valor`, `quantidade` | finance |
| `agg_pagamentos_diaria` | pagamentos diarios | `category`, `label`, `total_valor`, `qtd_comprovantes` | payments |
| `agg_pagamentos_turno` | pagamentos por turno | `id_turno`, `category`, `label`, `total_valor` | payments |
| `pagamentos_anomalias_diaria` | anomalias pagamento | `event_type`, `severity`, `score`, `insight_id_hash` | payments, notifications sync |
| `agg_caixa_turno_aberto` | caixa aberto | `id_turno`, `opened_at`, `expected_total`, `observed_total` | cash |
| `agg_caixa_forma_pagamento` | caixa por forma | `tipo_forma`, `forma_label`, `forma_category`, `total_valor` | cash |
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
- `db_clickhouse.py` nao usa singleton global do clickhouse-connect: cada query/insert abre client proprio e fecha no fim. Isso evita erro produtivo `Attempt to execute concurrent queries within the same session` em endpoints BI paralelos.
- Docker local/prod tem ClickHouse como servico de primeira classe.
- `MaterializedPostgreSQL` nao e mais caminho produtivo para `torqmind_dw`. Em producao ele deixou `fact_venda_item` vazia/inconsistente apesar do PostgreSQL estar correto e da table function `postgresql(...)` conseguir ler todos os dados.
- `torqmind_dw` agora e banco ClickHouse nativo (`Atomic`) com tabelas `MergeTree`/`ReplacingMergeTree` gravaveis. O sync controlado e feito por `deploy/scripts/prod-clickhouse-sync-dw.sh`.
- Full refresh ClickHouse deve dropar MVs streaming antes de recarregar `torqmind_dw`, recriar tabelas mart, rodar `phase3_native_backfill.sql` e so entao recriar MVs streaming.
- Frescor operacional ClickHouse separa cobertura comercial (`commercial_coverage.latest_available_dt`), publicacao tecnica (`operational_sync.last_sync_at`) e frescor de tela (`freshness.live_through_at`).
- `prod-clickhouse-init.sh` deve validar count/max(data_key) de `torqmind_dw.fact_venda` e `torqmind_dw.fact_venda_item` contra PostgreSQL antes de criar/backfillar marts.
- `prod-data-reconcile.sh` retorna exit code `1` apenas para divergencia critica; itens orfaos no DW PostgreSQL sao WARN de qualidade e nao gatilho automatico de rebuild.
- Frontend nunca deve montar filtro BI com `toISOString().slice(0, 10)`; datas de negocio ficam como string `YYYY-MM-DD`.
- `prod-etl-pipeline.sh` nunca roda full refresh: quando o track operational/risk produz mudancas, chama somente `MODE=incremental`.
- Incremental DW usa delete da janela afetada nas facts (`data_key`) e insert por `postgresql(...)`; dimensoes entram como novas versoes em `ReplacingMergeTree`.
- Incremental mart e idempotente por delete/reinsert da janela afetada antes de inserir agregados. Rodar duas vezes seguidas nao deve duplicar resultado.
- O sync incremental derruba MVs streaming antes de inserir no DW nativo para evitar escrita paralela/duplicada em marts; a publicacao operacional oficial e o refresh controlado. Full bootstrap pode recriar as MVs.
- Frescor simples para UI vem de `torqmind_ops.sync_state` com `name='mart_publication'`: `available`, `last_success_at`, `data_through_dt`, `source='clickhouse_mart'`, `mode`.
- Dashboard geral faz auto retry curto para payload instavel: 2s, ate 4 tentativas, cancelando quando o usuario troca filtro.
- Textos visiveis ao cliente evitam jargoes como mart/snapshot/recorte/trilho; exemplo: `Saidas normais` virou `Vendas normais`.
- Alertas futuros devem entrar depois do ETL operational e/ou depois da publicacao incremental ClickHouse, usando eventos/marts leves sem full refresh.
- `app.payment_type_map` e a fonte oficial de nomes/categorias de formas de pagamento. O sync DW carrega essa tabela app em `torqmind_dw.dim_forma_pagamento`; as marts guardam `label`/`category` reais e so usam `Forma nao identificada` quando nao ha mapeamento.
- Labels humanos de filial, operador, funcionario e local devem ser resolvidos no refresh/backfill das marts, nao por JOIN pesado na API. Objetos enriquecidos: `fraude_cancelamentos_eventos`, `risco_turno_local_diaria` e view `risco_eventos_recentes`.
- `pricing_competitor_overview` e fluxo app/transacional: continua ligado a PostgreSQL para valores salvos em `app.competitor_fuel_prices` e bypassa snapshot cache para GET apos POST nao devolver payload antigo.
- `cash_dre_summary` agora tem implementacao ClickHouse baseada em `finance_aging_daily`; ausencia de base retorna cards indisponiveis com `dt_ref=None`, nunca `1970-01-01`.

## 8. Pontas soltas resolvidas

- Rotas nao ficam mais presas acidentalmente ao repositorio PostgreSQL legado.
- Cliente ClickHouse corrigido para timeouts aceitos por `clickhouse-connect`.
- `query_dict()` corrigido para linhas dict, com teste unitario.
- Marts de produto passaram a carregar `custo_total`.
- `risco_eventos_recentes` passou a expor campos exigidos pelo backend.
- Compose local/prod agora inclui ClickHouse e variaveis da API.
- Makefile ganhou init/backfill/smoke ClickHouse.
- Reset SQL alinhado ate a migration `070`.
- Reparados constraints/indices necessarios para upserts e compatibilidade de replica identity legada.
- Reparada sincronizacao de notificacoes de anomalia de pagamento.
- Sanitizado float nao finito vindo de agregacoes ClickHouse antes de montar JSON.
- Corrigido client ClickHouse para nao compartilhar a mesma sessao entre threads FastAPI; `query_dict`, `query_scalar` e `insert_batch` usam client independente por contexto.
- Corrigido frescor de vendas para usar `agg_vendas_diaria.updated_at` com conversao UTC -> `America/Sao_Paulo`.
- Corrigido caixa para nao devolver `1970-01-01T00:00:00` quando nao ha linha util em `agg_caixa_turno_aberto`.
- Corrigido frontend para nao fixar sync indisponivel quando existe cobertura comercial publicada.
- Substituido bootstrap ClickHouse produtivo baseado em `MaterializedPostgreSQL` por sync nativo controlado PostgreSQL DW -> ClickHouse DW.
- Corrigido bootstrap ClickHouse prod para validar `fact_venda` e `fact_venda_item` completas antes do backfill das marts.
- Corrigido backfill historico para aceitar refresh com muitas particoes em uma sessao controlada.
- Adicionado script de reconciliacao `prod-data-reconcile.sh`.
- Adicionado fluxo operacional incremental ClickHouse: `prod-clickhouse-sync-dw.sh MODE=incremental`, `prod-clickhouse-refresh-marts.sh MODE=incremental`, pipeline com lock/timeout e cron default 2 min.
- Desativado refresh global de marts PostgreSQL legadas por default (`REFRESH_LEGACY_PG_MARTS=false`).
- Adicionados auditor historico e relatorio de orfaos, ambos baseados em `Comprovantes`/`ItensComprovantes`.
- Frontend ganhou auto retry para primeira carga sem payload estavel e copy mais simples de frescor.
- Corrigido `prod-etl-incremental.sh` para nao passar mensagens gigantes como argumento Python; pipeline agora extrai resumo JSON sem estourar `Argument list too long`.
- Corrigidas marts de pagamentos/caixa para usar labels reais de `app.payment_type_map` via `torqmind_dw.dim_forma_pagamento`, removendo `FORMA_X` do caminho produtivo.
- Corrigidas marts/view de antifraude e risco para carregar `filial_nome`, `usuario_nome`, `usuario_source`, `turno_value` e `local_nome` quando as dimensoes existem.
- Corrigido DRE/resumo de caixa ClickHouse para retornar cards financeiros a partir de `finance_aging_daily` e nunca renderizar data 1970 quando nao ha snapshot.
- Corrigido fluxo de preco concorrente para bypassar snapshot cache no overview e preservar inputs digitados enquanto o frontend refaz a consulta apos salvar.
- Adicionado `docs/clickhouse_semantic_parity_audit.md` e `prod-semantic-marts-audit.sh` para validar paridade semantica operacional.

## 9. Pontas soltas remanescentes

- Ainda nao existe mart ClickHouse para estoque, inadimplencia por cliente, precificacao concorrente analitica e projecao mensal de metas.
- Alguns testes legados criam dados diretamente no PostgreSQL e esperam leitura imediata em endpoints BI; com `USE_CLICKHOUSE=true`, esses testes precisam popular ClickHouse ou rodar com `USE_CLICKHOUSE=false`.
- O `make test` completo ainda nao esta verde: restam falhas de ETL/fixtures legadas e smokes que batem no servidor externo com estado diferente do processo de teste.
- Cobertura e2e Docker com autenticacao real dos endpoints `/bi/*` ainda deve ser consolidada com dados seedados representativos.
- Queries ClickHouse foram mantidas sem JOIN pesado no hot path, mas o tuning de ORDER BY/TTL/projecoes deve ser revisto com cardinalidade real de producao.
- No compose local, o track risk falhou por fixture/schema antigo sem constraint `uq_fact_risco_evento_nk`; pipeline registra falha e nao avanca janela risk. Validar migration prod antes de religar risk amplo.
- A primeira execucao incremental apos bootstrap antigo pode escolher janela grande se nao houver estado recente em `torqmind_ops.sync_state`; ciclos seguintes tendem a ser pequenos.
- Como o incremental derruba MVs streaming para garantir idempotencia, decidir em producao se as MVs continuam necessarias ou se o refresh controlado passa a ser o unico mecanismo operacional.

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
- Ao mexer em `db_clickhouse.py`, manter teste concorrente com `ThreadPoolExecutor`; nunca reintroduzir client singleton compartilhado.
- Ao mexer em vendas, confirmar que ETL ativo usa `stg.comprovantes`/`stg.itenscomprovantes`, nao `stg.movprodutos`.
- Ao mexer em datas no frontend, adicionar teste cobrindo horario noturno em `America/Sao_Paulo`.
- Nunca usar `DATABASE ENGINE = MaterializedPostgreSQL` no caminho produtivo de `torqmind_dw`; use DW nativo + sync explicito via `postgresql(...)`.
- Nunca colocar `prod-clickhouse-init.sh` ou `MODE=full` no cron operacional.
- Ao mexer no pipeline, validar lock, timeout, ausencia de senha em log e que orfaos continuam WARN.
- Ao mexer em pagamentos/caixa, manter `app.payment_type_map -> torqmind_dw.dim_forma_pagamento -> marts` e rodar `prod-semantic-marts-audit.sh`.
- Ao mexer em antifraude/risco, preservar labels desnormalizados de filial/operador/local nas marts; fallback "nao identificado" so quando a dimensao realmente nao existir.
- Preco concorrente e app-owned: POST salva em PostgreSQL e GET nao deve depender de snapshot stale.

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
- Revisao nativa DW 2026-04-29: `make clickhouse-init` passou criando `torqmind_dw` nativo com 14 tabelas obrigatorias; `fact_venda_item` bateu PostgreSQL em `4014480|20260429` no compose local.
- Revisao nativa DW 2026-04-29: `make migrate` passou com 0 novas migrations, 72 ja aplicadas e verificacao de schema OK.
- Revisao nativa DW 2026-04-29: `make clickhouse-native-backfill` passou apos recriar marts; `phase3_native_backfill.sql` agora permite muitas particoes somente na sessao do backfill.
- Revisao nativa DW 2026-04-29: `make clickhouse-mvs`, `make clickhouse-smoke` e `make analytics-smoke` passaram; `torqmind_dw=14`, `torqmind_mart=51`, facade com 68 funcoes.
- Revisao nativa DW 2026-04-29: `ALLOW_INSECURE_ENV=1 ENV_FILE=.env COMPOSE_FILE=docker-compose.yml ./deploy/scripts/prod-clickhouse-init.sh` passou end-to-end com sync DW nativo, backfill mart e MVs.
- Revisao nativa DW 2026-04-29: `ALLOW_INSECURE_ENV=1 ENV_FILE=.env COMPOSE_FILE=docker-compose.yml ID_EMPRESA=1 ID_FILIAL=14458 ./deploy/scripts/prod-data-reconcile.sh` passou com `errors=0 warnings=1`; os `83` itens orfaos sao WARN.
- Revisao nativa DW 2026-04-29: `docker compose exec -T api python -m unittest app.test_repos_analytics_unit` passou, 14 testes.
- Revisao nativa DW 2026-04-29: `docker compose exec -T web npm test` passou, 74 testes.
- Revisao nativa DW 2026-04-29: `docker compose -f docker-compose.prod.yml --env-file .env.production.example config --quiet` passou.
- Revisao nativa DW 2026-04-29: `make lint` passou com build Next.js.
- Revisao client CH 2026-04-29: `docker compose exec -T api python -m unittest app.test_db_clickhouse_unit app.test_repos_analytics_unit` passou, 19 testes, incluindo concorrencia com `ThreadPoolExecutor`.
- Revisao client CH 2026-04-29: `make analytics-smoke` passou com 68 funcoes no facade.
- Revisao client CH 2026-04-29: `make lint` passou com build Next.js.
- Revisao operacional incremental 2026-04-29: `bash -n deploy/scripts/prod-clickhouse-sync-dw.sh deploy/scripts/prod-clickhouse-refresh-marts.sh deploy/scripts/prod-etl-pipeline.sh deploy/scripts/prod-install-cron.sh deploy/scripts/prod-history-coverage-audit.sh deploy/scripts/prod-sales-orphans-report.sh deploy/scripts/prod-clickhouse-init.sh deploy/scripts/prod-data-reconcile.sh` passou.
- Revisao operacional incremental 2026-04-29: `python3 apps/api/app/test_clickhouse_operational_scripts_unit.py` passou, 5 testes.
- Revisao operacional incremental 2026-04-29: `PYTHONPATH=apps/api python3 -m unittest app.test_db_clickhouse_unit app.test_repos_analytics_unit app.test_sales_overview_unit` passou, 31 testes.
- Revisao operacional incremental 2026-04-29: `docker compose exec -T api python -m unittest app.test_db_clickhouse_unit app.test_repos_analytics_unit app.test_sales_overview_unit` passou, 31 testes.
- Revisao operacional incremental 2026-04-29: `docker compose exec -T web npm test` passou, 76 testes.
- Revisao operacional incremental 2026-04-29: `make analytics-smoke` passou com 68 funcoes no facade.
- Revisao operacional incremental 2026-04-29: `make lint` passou com build Next.js.
- Revisao operacional incremental 2026-04-29: `docker compose -f docker-compose.prod.yml --env-file .env.production.example config --quiet` passou.
- Revisao operacional incremental 2026-04-29: sync DW incremental local detectou janela grande `20250423-20260429`, validou `fact_venda`/`fact_venda_item` e o refresh mart incremental concluiu; uma execucao sem mudancas retornou `window=0-0`.
- Revisao operacional incremental 2026-04-29: `prod-data-reconcile.sh` passou com `errors=0 warnings=1`; `83` itens orfaos continuam WARN.
- Revisao operacional incremental 2026-04-29: `prod-history-coverage-audit.sh` passou e mostrou STG canonico desde `2025-01-01`, DW/mart de vendas da filial `14458` desde `2025-04-21/2025-04-22`.
- Revisao operacional incremental 2026-04-29: `prod-sales-orphans-report.sh` passou, reportando `orphan_items=83` sem deletar dados.
- Revisao operacional incremental 2026-04-29: simulacao de `prod-etl-pipeline.sh` validou timeout/skip; operational local excedeu 20s e risk local falhou por constraint ausente de fixture, sem rodar full refresh nem avancar janela risk.
- Revisao paridade semantica 2026-04-30: `bash -n deploy/scripts/prod-clickhouse-sync-dw.sh deploy/scripts/prod-clickhouse-refresh-marts.sh deploy/scripts/prod-clickhouse-init.sh deploy/scripts/prod-semantic-marts-audit.sh deploy/scripts/prod-data-reconcile.sh` passou.
- Revisao paridade semantica 2026-04-30: `MODE=incremental DT_INI=2026-04-29 DT_FIM=2026-04-29 prod-clickhouse-sync-dw.sh` passou em compose local, carregando `dim_forma_pagamento` e validando janelas DW.
- Revisao paridade semantica 2026-04-30: `MODE=incremental DT_INI=2026-04-29 DT_FIM=2026-04-29 prod-clickhouse-refresh-marts.sh` passou apos corrigir aliases de subquery ClickHouse.
- Revisao paridade semantica 2026-04-30: `make clickhouse-marts-init && make clickhouse-native-backfill && make clickhouse-mvs` passou e validou o backfill full das marts com labels humanos.
- Revisao paridade semantica 2026-04-30: `prod-semantic-marts-audit.sh ID_EMPRESA=1 ID_FILIAL=14458` passou com `errors=0 warnings=0`; nenhum `FORMA_*`, filial/usuario perdidos ou data 1970.
- Revisao paridade semantica 2026-04-30: `prod-data-reconcile.sh ID_EMPRESA=1 ID_FILIAL=14458` passou com `errors=0 warnings=1`; os `83` itens orfaos permanecem WARN.
- Revisao paridade semantica 2026-04-30: `PYTHONPATH=apps/api python3 -m unittest app.test_clickhouse_operational_scripts_unit app.test_repos_analytics_unit app.test_snapshot_cache` passou, 50 testes.
- Revisao paridade semantica 2026-04-30: `docker compose exec -T api python -m unittest app.test_db_clickhouse_unit app.test_repos_analytics_unit app.test_sales_overview_unit app.test_clickhouse_operational_scripts_unit app.test_snapshot_cache` passou, 67 testes, 7 skips.
- Revisao paridade semantica 2026-04-30: `docker compose exec -T web npm test` passou, 76 testes; `cd apps/web && npm test` passou, 77 testes no host.
- Revisao paridade semantica 2026-04-30: `docker compose -f docker-compose.prod.yml --env-file .env.production.example config --quiet`, `make clickhouse-smoke`, `make analytics-smoke` e `make lint` passaram.
- Revisao estabilizacao UX/pipeline 2026-04-30: `bash -n` dos scripts de pipeline/sync/refresh/cron passou; `make analytics-smoke` passou; `docker compose exec -T api python -m unittest app.test_db_clickhouse_unit app.test_repos_analytics_unit app.test_sales_overview_unit` passou (34 testes); `cd apps/web && npm test` passou (79 testes); `docker compose -f docker-compose.prod.yml --env-file .env.production.example config --quiet` passou; `make lint` falhou localmente por `service "web" is not running`.
- Revisao antifraude/entrega final 2026-04-30: `docker compose exec -T clickhouse clickhouse-client --query "SHOW CREATE TABLE torqmind_mart.risco_eventos_recentes"` confirmou aliases limpos (`id_filial`, `data_key`, `updated_at` etc.) apos reaplicar a view no DB correto; `docker compose exec -T api python -m unittest app.test_smoke_api.SmokeApiTest.test_fraud_overview_endpoint_returns_contract` passou; `ALLOW_INSECURE_ENV=1 ENV_FILE=.env COMPOSE_FILE=docker-compose.yml ID_EMPRESA=1 ID_FILIAL=14458 ./deploy/scripts/prod-semantic-marts-audit.sh` passou com `errors=0 warnings=0`; `prod-data-reconcile.sh` passou com `errors=0 warnings=1`.

## 13. Ajustes operacionais e UX (2026-04-30)

- Pipeline `prod-etl-pipeline.sh` agora publica ClickHouse incremental sempre que o trilho operacional/risk conclui com sucesso, mesmo quando a flag `phase_domains/clock_meta` vier vazia; a detecção final de delta fica no `prod-clickhouse-sync-dw.sh` (janela por `updated_at` + estado em `torqmind_ops.sync_state`).
- Lock anti-overlap continua com `flock -n` e ganhou log de idade do lock (`age=...s`) para diagnosticar lock stale no host sem destravar na marra.
- Vendas operacionais não fazem mais shift automático para “ontem”: em `requested_dt_ini > latest_available_dt`, `commercial_window_coverage.mode=requested_outside_coverage` e o recorte efetivo permanece o solicitado.
- Estado “Todas as filiais” foi estabilizado no frontend com sentinel explícito `branch_scope=all` em URL/estado; ao trocar de aba não expande mais para lista de `id_filiais`.
- UX cliente: removidos banners/cards de frescor técnico das telas de produto (`dashboard`, `sales`, `cash`, `fraud`, `customers`, `finance`, `pricing`, `goals`) e também a seção lateral de `Frescor operacional` do `AppNav`. Informações técnicas devem ficar apenas na área de `Plataforma`.
- Terminologia BR: botão superior “Platform” no produto virou “Plataforma”; labels de papel também foram traduzidos (`Plataforma Master`, `Administrador da Plataforma`, etc).
- Regras preservadas: origem canônica de vendas continua `stg.comprovantes`/`stg.itenscomprovantes`; nenhum retorno `1970-01-01` foi reintroduzido; fluxo de preço concorrente segue app-owned em PostgreSQL.
- Antifraude 500 real: a view `torqmind_mart.risco_eventos_recentes` existia no ClickHouse com nomes de coluna qualificados (`r.id_filial`, `r.data_key`, `r.created_at`) porque o SELECT da view não dava alias explícito para colunas brutas. A API consulta `id_filial`, então `/bi/fraud/overview` quebrava com `UNKNOWN_IDENTIFIER`. O contrato correto agora exige aliases explícitos e colunas: `id`, `id_empresa`, `id_filial`, `filial_nome`, `data_key`, `data`, `event_type`, `id_db`, `id_comprovante`, `id_movprodutos`, `id_usuario`, `id_funcionario`, `funcionario_nome`, `id_turno`, `turno_value`, `operador_caixa_id`, `operador_caixa_nome`, `operador_caixa_source`, `id_cliente`, `valor_total`, `impacto_estimado`, `score_risco`, `score_level`, `reasons`, `created_at`, `updated_at`.
- Diagnóstico rápido de antifraude em produção/homologação:
  `SHOW CREATE TABLE torqmind_mart.risco_eventos_recentes`
  Se aparecerem colunas como `` `r.id_filial` `` ou `` `r.data_key` ``, o schema da view está quebrado e precisa de full init ou reaplicação da view no DB `torqmind_mart`.
- Mudança de schema da mart/view de risco exige refresh completo recomendado:
  `ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-clickhouse-init.sh`
  Depois validar com `prod-data-reconcile.sh` e `prod-semantic-marts-audit.sh`.

## 12. Regras de ouro

- Nunca remover filtro `id_empresa`.
- Nunca quebrar JSON do frontend.
- Preferir `torqmind_mart`.
- Nao mascarar erro critico com zero silencioso.
- Manter fallback explicito via `USE_CLICKHOUSE=false`.
- Atualizar testes junto com contrato, SQL e facade.
- Nunca reintroduzir `stg.movprodutos` como fonte canonica de vendas.
- Infra UTC; negocio/UI `America/Sao_Paulo`; filtros sempre `YYYY-MM-DD`; API retorna timestamp tecnico com offset.
- Bootstrap ClickHouse: sincronizar `torqmind_dw` nativo e validar count/max(data_key) contra PostgreSQL antes de backfillar marts.
- Nunca compartilhar uma instancia global de client clickhouse-connect entre threads; o client carrega estado de sessao.

## 14. Arquitetura Event-Driven Streaming (TorqMind 2.0)

Status: FUNDAÇÃO CRIADA — stack paralela ao sistema atual.

### Decisões arquiteturais

- Broker: Redpanda (Kafka-compatible, single binary, leve em memória).
- CDC: Debezium PostgreSQL Connector via Debezium Connect.
- Consumer: serviço Python próprio (`apps/cdc_consumer/`) — controle total sobre mapeamento, idempotência e observabilidade.
- OLAP: ClickHouse com 4 camadas: `torqmind_raw`, `torqmind_current`, `torqmind_ops`, `torqmind_mart`.
- NÃO usa MaterializedPostgreSQL.
- NÃO corta pipeline atual (cron/shell ETL) nesta fase.
- NÃO troca API para `torqmind_current` ainda.

### Fluxo CDC

```
PostgreSQL (dw.*, app.*) → Debezium → Redpanda → CDC Consumer → ClickHouse (raw/current/ops)
```

### Tabelas capturadas (primeiro escopo)

Facts: `fact_venda`, `fact_venda_item`, `fact_pagamento_comprovante`, `fact_caixa_turno`, `fact_comprovante`, `fact_financeiro`, `fact_risco_evento`.
Dims: `dim_filial`, `dim_produto`, `dim_grupo_produto`, `dim_funcionario`, `dim_usuario_caixa`, `dim_local_venda`, `dim_cliente`.
App: `payment_type_map`.

### Tópicos Redpanda

Formato: `torqmind.<schema>.<table>` (ex: `torqmind.dw.fact_venda`).

### Idempotência

- Raw: append-only com projeção dedup por (topic, partition, offset).
- Current: ReplacingMergeTree com version = source_ts_ms; mesma PK com ts_ms menor é descartada no merge.
- Deletes: marcados com `is_deleted=1`, não há DELETE físico.

### Mapa de arquivos streaming

- `docker-compose.streaming.yml`: stack Redpanda + Console + Debezium + Consumer.
- `apps/cdc_consumer/`: serviço CDC Consumer Python.
- `deploy/debezium/connectors/torqmind-postgres-cdc.json`: config do connector.
- `deploy/scripts/streaming-*.sh`: scripts operacionais.
- `sql/clickhouse/streaming/001_databases.sql`: cria databases.
- `sql/clickhouse/streaming/010_raw_events.sql`: tabela raw append-only.
- `sql/clickhouse/streaming/020_current_tables.sql`: tabelas current (ReplacingMergeTree).
- `sql/clickhouse/streaming/030_ops_tables.sql`: tabelas operacionais (offsets, lag, erros).
- `docs/architecture/TORQMIND_EVENT_DRIVEN_2_0.md`: documentação completa.
- `docs/architecture/TORQMIND_2_0_CUTOVER_PLAN.md`: plano de migração batch → streaming.
- `docs/product/TORQMIND_PRODUCT_WORLD_CLASS_AUDIT.md`: auditoria world-class de produto.

### Variáveis de ambiente streaming (CDC Consumer)

- `REDPANDA_BROKERS`, `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`
- `CDC_CONSUMER_GROUP`, `CDC_TOPICS`, `CDC_TOPIC_PATTERN`, `CDC_BATCH_SIZE`, `CDC_FLUSH_INTERVAL_SECONDS`
- `LOG_LEVEL`

### Comandos Makefile streaming

- `make streaming-up`: sobe stack.
- `make streaming-down`: para stack.
- `make streaming-init-clickhouse`: cria schemas ClickHouse.
- `make streaming-register-debezium`: registra connector.
- `make streaming-status`: status geral.
- `make streaming-validate-cdc`: valida pipeline.
- `make streaming-logs`: tail logs.
- `make streaming-config-check`: valida compose.
- `make test-cdc-consumer`: testes unitários do consumer.

### Próximos passos (não implementados nesta rodada)

- CDC rodando em produção com validação completa.
- API consultando `torqmind_current` para endpoints selecionados.
- Marts streaming substituindo marts batch.
- Observabilidade com Prometheus/Grafana.
- Flink para CEP/aggregation streaming.
- Temporal para workflows de backfill/onboarding.
- Agent consumindo eventos para alertas reativos.

### Perfis de deploy

- `local-full`: Redpanda + Console + Debezium + Consumer (dev).
- `prod-lite`: Redpanda + Debezium + Consumer sem Console (4 CPU / 8 GB).

### Validações executadas (2026-04-30)

- `bash -n deploy/scripts/streaming-*.sh`: passou.
- `docker compose -f docker-compose.streaming.yml --profile local-full config --quiet`: passou.
- `docker compose -f docker-compose.prod.yml --env-file .env.production.example config --quiet`: passou (não quebrou).
- `python3 -m pytest apps/cdc_consumer/tests/ -v`: 29 testes + 15 subtestes passaram.
- DDL alignment tests verificam que 010/020/030 SQL correspondem exatamente ao que `clickhouse_writer.py` insere.
- Rede Docker corrigida: `torqmind_default` (external) em vez de rede isolada.
- Scripts de streaming com auth ClickHouse e env POSTGRES_* padronizado.

## 15. Auditoria de Produto World-Class (2026-04-30)

Status: AUDITORIA COMPLETA — documentos criados.

### Documentos produzidos

- `docs/product/TORQMIND_PRODUCT_WORLD_CLASS_AUDIT.md`: auditoria tela por tela, domínio por domínio, personas, quick wins, roadmap.
- `docs/architecture/TORQMIND_2_0_CUTOVER_PLAN.md`: plano de migração batch → streaming em 7 fases com rollback e checklists.

### Achados críticos de produto

1. ~~**"recorte"** em 32 ocorrências no frontend~~ — **RESOLVIDO**: substituído por "período" em todo frontend e backend.
2. ~~**"não identificado"** em 8 ocorrências~~ — **RESOLVIDO**: substituído por termos contextuais (sem cadastro, sem classificação).
3. ~~JWT secret default aceito silenciosamente em produção~~ — **RESOLVIDO**: fail-fast bloqueia placeholders + padrões fracos em prod/homolog/staging.
4. ~~`product_global` role não valida tenant_id~~ — **RESOLVIDO**: valida tenant_ids; sem tenants = 403.
5. ~~24 endpoints BI sem response_model tipado~~ — **FASE 1 RESOLVIDA**: 5 endpoints principais com envelope tipado (Dict/Any + extra allow). Próxima fase: contratos fortes por domínio.
6. Falta mart de divergência de caixa, inadimplência por cliente, histórico de preço.
7. Plataforma admin sem health técnico de CDC/streaming.

### Response Models — Estado atual (Fase 1)

Os 5 endpoints BI principais (`/dashboard/home`, `/sales/overview`, `/cash/overview`, `/fraud/overview`, `/finance/overview`) usam `response_model` com modelos envelope (`CacheMetadata` + campos tipados como `Dict[str, Any]` + `model_config = {"extra": "allow"}`).

No antifraude, o campo externo continua sendo `model_coverage`, mas o schema usa um nome interno com alias para evitar warning de namespace protegido do Pydantic.

**Justificativa Fase 1:** os payloads reais são dinâmicos e variam por contexto de negócio. Os modelos envelope garantem documentação OpenAPI e serialização controlada sem quebrar contratos existentes.

**Fase 2 (futura):** contratos fortes por domínio com schemas aninhados específicos (KPI models, Series models, etc.) quando os payloads estabilizarem.

### Ingest Key — Decisão de Design

- **Header canônico:** `X-Ingest-Key` (único, sem alias).
- Em produção/homolog/staging: `INGEST_REQUIRE_KEY=true` obrigatório (enforced via fail-fast).
- Sem chave → 401 "Missing X-Ingest-Key".
- Chave inválida → 401 "Invalid X-Ingest-Key".
- Chave válida → resolve `id_empresa` do tenant.

### Security Gates — Ambientes produtivos

Ambientes bloqueados: `prod`, `production`, `homolog`, `homologation`, `staging`.

Validações em `_validate_production_settings()`:
- JWT secret: rejeita vazio, placeholders (CHANGE_ME*), valores triviais (password, admin, 1234, etc.) e qualquer valor com menos de 32 caracteres.
- PG password: mesma regra
- ClickHouse: `CLICKHOUSE_USER=default` é proibido em ambiente produtivo, mesmo com senha forte; `CLICKHOUSE_PASSWORD` vazio/placeholder também é bloqueado.
- INGEST_REQUIRE_KEY=true obrigatório
- Em dev/test/local os defaults continuam permitidos, mas com warning explícito para não mascarar risco real.
- Não há validação artificial para `ETL_INTERNAL_KEY` no startup: o header canônico de ingest continua sendo `X-Ingest-Key`, resolvido por tenant no banco.

### Quick wins identificados

- ~~Replace "recorte" → "período"~~ ✅
- ~~Replace "não identificado" → termos contextuais~~ ✅
- ~~Fail-fast config.py se JWT secret = default em produção~~ ✅
- ~~Validar id_empresa em product_global scope~~ ✅

### UI Copy Quality Gate

Teste automatizado `apps/web/app/lib/ui-copy-quality.test.mjs`:
- Integrado ao `npm test` (roda no pipeline).
- Escaneia recursivamente `.tsx`, `.ts`, `.mjs`, `.js`.
- Exclui arquivos de teste (*.test.*, *.spec.*) e diretórios node_modules/.next.
- Reporta arquivo, linha, termo, motivo e trecho.
- Termos proibidos: `recorte`, `não identificado/a/os`, `Saídas normais`, `Frescor operacional`, `FORMA_`, `01/01/1970`, `1970` em texto visível, `mart`, `snapshot`, `trilho operacional`, `publicação analítica` e `Platform` como label visual.
- Usa heurística específica para `Platform` e allowlist curta para arquivos técnicos internos de leitura/cobertura.
- Qualquer novo termo proibido detectado falha o build.

### Runtime / Container Validation

Checklist mínimo para garantir que o container API reflete a branch atual:
- `docker compose build api`
- `docker compose up -d api`
- `docker compose exec -T api python - <<'PY'`
  `from app import schemas_bi`
  `print([name for name in dir(schemas_bi) if name.endswith("Response")])`
  `PY`
- `make analytics-smoke`

### Cutover streaming: estado

| Fase | Status |
|------|--------|
| 1 - CDC Paralelo | ✅ Completa |
| 2 - Validação em staging | Próxima |
| 3 - Mart piloto streaming | Planejada |
| 4 - API feature flag | Planejada |
| 5 - Dashboards migrados | Planejada |
| 6 - Alertas | Planejada |
| 7 - Agent/Jarvis | Planejada |

### Correção técnica (streaming)

- `040_pilot_marts.sql` removido do disco (não existia mais); referência mantida no Section 14 como histórico.
- Rede Docker streaming corrigida para `external: true` com `torqmind_default`.
- Env vars padronizados para POSTGRES_* com fallback PG_*.
- ClickHouse auth adicionado em todos os scripts.
- Register Debezium usa Python JSON generation (não sed).
- CDC_TOPIC_PATTERN corrigido: `^torqmind\..*` (escape simples no YAML).
- `python3 -m pytest apps/cdc_consumer/tests/ -v`: 29 testes, todos passaram.
- Scripts não imprimem senhas (validado com grep).
