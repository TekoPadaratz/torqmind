# TorqMind Product Screen Audit

Data: 2026-05-04
Ambiente: homolog oficial
Branch: nova-branch-limpa
Workspace: /home/deploy/torqmind

## Objetivo

Fechar uma rodada de produto com foco em dois critérios não negociáveis:

- não devolver 200 com tela materialmente vazia
- servir dados reais já carregados em STG, DW e ClickHouse, sem copy ou contratos improvisados

## Resultado desta rodada

### Correções aplicadas e validadas

| Tela/domínio | Sintoma observado | Causa raiz | Correção aplicada | Evidência runtime |
| --- | --- | --- | --- | --- |
| Dashboard Geral | snapshot write quebrava a rota quente | `NaN/Inf` chegavam ao JSONB do cache | normalização recursiva de números não finitos em `snapshot_cache` | `/api/bi/dashboard/home` voltou a 200 e gravou snapshot novo |
| Antifraude operacional | `kpis.cancelamentos` e `kpis.valor_cancelado` vinham nulos | `fraud_kpis()` realtime devolvia `qtd_eventos/impacto_total` em vez do contrato operacional esperado pela UI | contrato alinhado com aliases operacionais e invalidação do snapshot da rota | `/api/bi/fraud/overview` devolveu `cancelamentos=445` e `valor_cancelado=515720.53` |
| Antifraude pagamentos | `payments_risk[].filial_label` caía em `Filial sem cadastro` para filial real | `payments_anomalies()` vinha com `filial_nome=''` e não fazia backfill no ClickHouse path | enriquecimento com nome atual da filial a partir de `torqmind_current.stg_filiais` | amostra real passou a devolver `AUTO POSTO VR 05` |
| Caixa | aliases legados `recebimentos_periodo` e `cancelamentos_periodo` vinham nulos | payload publicava só `total_pagamentos` e `total_cancelamentos` | aliases adicionados em realtime, ClickHouse e PostgreSQL, com invalidação do snapshot da rota | `/api/bi/cash/overview` devolveu aliases preenchidos no topo e em `historical.kpis` |
| Plataforma | `/platform/streaming-health` retornava 500 | rota chamava `repos_platform.require_platform_access`, helper inexistente | troca para `repos_auth.assert_platform_access()` e teste de regressão | `/api/platform/streaming-health` voltou a 200 com payload de saúde |
| Metas & Equipe | a leitura ainda dependia de `app.goals` no PostgreSQL e a projeção mensal não podia sair do legado | faltavam `torqmind_current.goals`, mapping CDC e leitura ClickHouse para `goals_today`/`monthly_goal_projection` | criada `torqmind_current.goals`, adicionado mapping `app.goals`, bootstrap operacional e cutover das leituras de metas para ClickHouse usando `torqmind_current.goals` + `torqmind_dw.fact_venda` | `goals_today` real carregado no ClickHouse; em 2026-04-29 a projeção ClickHouse ficou aderente ao legado, e em 2026-04-30 ficou mais fresca porque a cobertura comercial do ClickHouse já alcançava o dia 30 |

## Evidências executadas

### Testes automatizados

- `python -m unittest app.test_snapshot_cache -q`
- `python -m unittest app.test_realtime_contracts -q`
- `python -m unittest app.test_platform_backoffice.PlatformBackofficeTest.test_streaming_health_requires_platform_access_and_returns_payload -q`
- `python -m unittest app.test_repos_analytics_unit.ClickHouseQueryScopeUnitTest.test_cash_dre_summary_uses_finance_mart_and_never_epoch_zero app.test_repos_analytics_unit.ClickHouseQueryScopeUnitTest.test_payments_anomalies_backfills_filial_name_from_current_snapshot -q`
- `python -m unittest app.test_repos_analytics_unit.AnalyticsFacadeUnitTest.test_inventory_marks_goal_reads_as_clickhouse app.test_repos_analytics_unit.ClickHouseQueryScopeUnitTest.test_goals_today_uses_clickhouse_current_table app.test_repos_analytics_unit.ClickHouseQueryScopeUnitTest.test_monthly_goal_projection_uses_clickhouse_goal_rows_and_sales_series -q`
- `python -m unittest app.test_realtime_contracts.TestCashOverviewRealtimeLabels.test_cash_uses_real_labels_when_current_dimensions_exist app.test_cash_payment_mix_unit.CashPaymentMixUnitTest.test_cash_historical_payment_mix_uses_dw_payment_facts_for_single_branch_period -q`

### Validações em runtime

- `/api/health` retornando 200 após cada rebuild crítico
- `/api/bi/dashboard/home` retornando 200 com snapshot novo após correção do cache
- `/api/bi/fraud/overview` retornando KPIs operacionais preenchidos e `payments_risk` com filial real
- `/api/bi/cash/overview` retornando aliases legados preenchidos com `contract_version=2`
- `/api/platform/streaming-health` retornando 200 com `use_realtime_marts=true`
- comparação runtime `repos_mart` vs `repos_mart_clickhouse` para Metas no tenant 1 / filial 14458: `goals_today` carregado com dado real de `app.goals`; em `as_of=2026-04-29` a projeção ClickHouse ficou numericamente próxima do legado, e em `as_of=2026-04-30` a leitura ClickHouse passou a refletir um dia a mais de cobertura comercial

## Ações operacionais desta rodada

### ETL e cron

- o pipeline manual foi disparado no homolog via `deploy/scripts/prod-etl-pipeline.sh`
- o cron ativo do host foi reinstalado para o repo atual com janela operacional de 2 em 2 minutos
- a investigação do travamento mostrou que a falha histórica estava no trilho `operational` (`fact_estoque_atual` ausente), não na trilha `risk`
- o pipeline passou a operar com `RISK_TRACK_MODE=auto`, pulando `risk` quando o ambiente está em `USE_REALTIME_MARTS=true`, `REALTIME_MARTS_SOURCE=stg` e sem refresh legado de marts PostgreSQL
- a execução operacional válida em homolog levou cerca de 139s; por isso o timeout do pipeline foi elevado para 240s e o limiar de warning para 120s, evitando abortar um ciclo saudável antes do fim
- a causa raiz do novo gargalo ficou identificada no SQL incremental: `load_fact_comprovante`, `load_fact_venda`, `load_fact_pagamento_comprovante_range_detail` e `load_fact_venda_item_range_detail` revarriam a hot window fixa de 3 dias mesmo com watermark já atualizado
- a correção foi versionada em `sql/migrations/073_incremental_sales_watermark_only.sql`, trocando o critério incremental desses loaders e dos `pending_bounds` para `received_at > watermark` ou `force_full_scan`, preservando o backfill explícito apenas quando realmente solicitado
- em homolog, antes da correção os bounds ficavam estáveis em cerca de `payment.candidate_refs ~= 16244` e `venda_item.candidate_rows ~= 26265`; após a migration, os `pending_bounds` passaram a zerar em rodada limpa e os deltas residuais observados ficaram curtos e drenáveis na própria execução seguinte
- o `operational` ainda executa `fast_path` para snapshots de cliente/churn quando há mudança de vendas; esse caminho não foi removido nesta rodada porque o produto atual ainda consome esse material em domínios fora do realtime STG
- a linha efetiva instalada ficou em:

```cron
*/2 * * * * cd /home/deploy/torqmind && ENV_FILE=/etc/torqmind/prod.env COMPOSE_FILE=docker-compose.prod.yml RISK_INTERVAL_MINUTES=30 RISK_TRACK_MODE=auto PIPELINE_TIMEOUT_SECONDS=240 PIPELINE_WARN_SECONDS=120 /home/deploy/torqmind/deploy/scripts/prod-etl-pipeline.sh >> /home/deploy/logs/torqmind-etl-pipeline.log 2>&1
```

- o instalador passou a usar `/home/deploy/logs/torqmind-etl-pipeline.log` como destino padrão

## Gaps ainda visíveis

- a página web de Plataforma continua orientada a backoffice comercial/financeiro; a saúde técnica de streaming já existe em API, mas ainda não está exposta como painel visual próprio
- Clientes e Financeiro não exigiram correção de contrato nesta rodada, mas devem entrar no smoke contínuo para evitar regressão silenciosa de materialidade
- Metas & Equipe segue com `goals_today` vazio para o recorte validado; a projeção mensal existe e a tela não está vazia, mas o comportamento diário precisa de leitura de negócio antes de qualquer mudança estrutural

## Arquivos alterados nesta rodada

- `apps/api/app/services/snapshot_cache.py`
- `apps/api/app/routes_bi.py`
- `apps/api/app/routes_platform.py`
- `apps/api/app/repos_mart_realtime.py`
- `apps/api/app/repos_mart_clickhouse.py`
- `apps/api/app/repos_mart.py`
- `apps/api/app/test_snapshot_cache.py`
- `apps/api/app/test_realtime_contracts.py`
- `apps/api/app/test_platform_backoffice.py`
- `apps/api/app/test_repos_analytics_unit.py`
- `apps/api/app/test_cash_payment_mix_unit.py`
- `apps/api/app/test_release_hardening.py`
- `deploy/scripts/prod-etl-pipeline.sh`
- `deploy/scripts/prod-install-cron.sh`
- `sql/migrations/073_incremental_sales_watermark_only.sql`

## Próximo passo recomendado

Executar o smoke de produto após cada rebuild ou publicação incremental, usando `deploy/scripts/realtime-product-screen-smoke.sh`, e tratar qualquer 200 sem materialidade como regressão de release.