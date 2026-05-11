# TorqMind Production Operating Map

Atualizado em: 2026-05-11 15:50 America/Sao_Paulo
Checkout de referência: nova-branch-limpa @ 9226e5f + ajustes operacionais locais

## Topologia

- PostgreSQL STG/DW: 172.30.0.8
- Analytics / ClickHouse / Redpanda / Debezium / CDC: 172.30.0.9
- App / API / Web / Nginx: 172.30.0.10
- URL pública: http://redevr.ddns.me:14023
- API pública: http://redevr.ddns.me:14023/api
- Timezone operacional: America/Sao_Paulo

## Serviços e compose

- Host App/API/Web/Nginx: docker-compose.app.yml
- Host PostgreSQL: docker-compose.pg.yml
- Host Analytics/Realtime: docker-compose.analytics.yml
- Envs de produção: /etc/torqmind/prod.app.env, /etc/torqmind/prod.pg.env, /etc/torqmind/prod.analytics.env, /etc/torqmind/cluster.env
- Checkout operacional em todos os hosts: /home/tm/torqmind
- Consumer group Kafka efetivo em produção: torqmind-cdc-consumer-live

## Fluxo de dados

- Agent Windows -> API ingest -> PostgreSQL STG -> ETL/DW -> Debezium/Redpanda/CDC -> ClickHouse current/slim -> mart_rt -> API/Web.
- Backfill do Agent deve materializar primeiro em STG; downstream só é reprocessado quando STG estiver completa.
- Bootstrap realtime usa STG como origem para current/slim.
- Marts realtime são rebuildadas a partir de slim/current após bootstrap consistente.

## Tabelas canônicas

- STG principal: stg.comprovantes, stg.itenscomprovantes, stg.formas_pgto_comprovantes, stg.nfe
- STG dimensões operacionais: stg.produtos, stg.grupoprodutos, stg.entidades, stg.turnos, stg.funcionarios, stg.usuarios, stg.localvendas
- DW principal: dw.fact_comprovante, dw.fact_venda, dw.fact_venda_item, dw.fact_pagamento_comprovante, dw.fact_financeiro, dw.fact_caixa_turno, dw.dim_cliente e dimensões correlatas
- ClickHouse current/slim: torqmind_current.stg_comprovantes_slim, torqmind_current.stg_itenscomprovantes_slim, torqmind_current.stg_formas_pgto_slim, torqmind_current.stg_nfe_slim
- Mart realtime: torqmind_mart_rt.dashboard_home_rt, sales_daily_rt, sales_hourly_rt, sales_products_rt, sales_groups_rt, payments_by_type_rt, cash_overview_rt, fraud_daily_rt, risk_recent_events_rt, finance_overview_rt, nfe_inutilizations_rt, source_freshness, mart_publication_log

## Regra fiscal NFE

- STATUS=3: autorizado
- STATUS=4: cancelamento real
- STATUS=5: inutilização
- NFE usa DATA como coluna temporal de backfill, incremental e filtro
- DATAREPL não deve ser usado como filtro temporal ou watermark de NFE
- DATAEMISSAO não existe na tabela NFE do cliente
- SERIE não existe na tabela NFE do cliente e não pode ser obrigatória
- STATUS=5 não entra em vendas, cancelamentos reais, fraude ou risco operacional
- STATUS=5 entra na mart torqmind_mart_rt.nfe_inutilizations_rt
- Documento de apoio semântico: docs/data/NFE_FISCAL_CLASSIFICATION.md

## Regra de vendas

- Faturamento é derivado de itens válidos
- Join preferencial por id_empresa, id_filial, id_db, id_comprovante quando disponível
- id_db é obrigatório em joins e chaves naturais
- Data da venda vem do comprovante
- Timezone de leitura e publicação: America/Sao_Paulo
- Documento de apoio semântico: docs/data/TORQMIND_SEMANTIC_FIELD_MAP.md

## Regras de Caixa

- Caixa 0 deve ser ignorado em rankings e relações por caixa
- Inutilizações fiscais aparecem como seção própria no Caixa
- Turnos e detalhes operacionais devem expor data e hora
- Cancelamentos reais no Caixa não incluem NFE status=5

## Scripts de operação

- deploy/scripts/prod-etl-incremental.sh
- deploy/scripts/realtime-bootstrap-stg.sh
- deploy/scripts/realtime-rebuild-mart-rt-from-slim.sh
- deploy/scripts/prod-multivm-validate.sh
- deploy/scripts/prod-multivm-proof.sh
- deploy/scripts/realtime-product-screen-smoke.sh

## Comandos seguros

- Validar containers App: docker compose -f docker-compose.app.yml --env-file /etc/torqmind/prod.app.env ps
- Validar containers PG: ssh tm@172.30.0.8 'cd /home/tm/torqmind && docker compose -f docker-compose.pg.yml --env-file /etc/torqmind/prod.pg.env ps'
- Validar containers Analytics: ssh tm@172.30.0.9 'cd /home/tm/torqmind && docker compose -f docker-compose.analytics.yml --env-file /etc/torqmind/prod.analytics.env ps'
- Validar STG: consultas SQL em stg.comprovantes, stg.itenscomprovantes, stg.formas_pgto_comprovantes, stg.nfe antes de qualquer rebuild downstream
- Rodar ETL: ENV_FILE=/etc/torqmind/prod.app.env ./deploy/scripts/prod-etl-incremental.sh
- Rodar bootstrap realtime: ENV_FILE=/etc/torqmind/prod.app.env CLICKHOUSE_HOST=172.30.0.9 PG_HOST=172.30.0.8 ./deploy/scripts/realtime-bootstrap-stg.sh
- Rebuild marts: ENV_FILE=/etc/torqmind/prod.app.env CLICKHOUSE_HOST=172.30.0.9 ./deploy/scripts/realtime-rebuild-mart-rt-from-slim.sh --drop-recreate --mart-only
- Validate multi-VM: ENV_FILE=/etc/torqmind/prod.app.env CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-validate.sh
- Proof multi-VM: ENV_FILE=/etc/torqmind/prod.app.env CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-proof.sh
- Smoke público: ENV_FILE=/etc/torqmind/prod.app.env PUBLIC_URL=http://redevr.ddns.me:14023 ./deploy/scripts/realtime-product-screen-smoke.sh

## Índice de referência

- Semântica de campos: docs/data/TORQMIND_SEMANTIC_FIELD_MAP.md
- Classificação fiscal NFE: docs/data/NFE_FISCAL_CLASSIFICATION.md
- Auditoria de telas: docs/audits/TORQMIND_PRODUCT_SCREEN_AUDIT.md
- Runbook do agent: docs/agent_runbook.md

## Último estado conhecido

- Containers App/API/Web/Nginx: healthy no host 172.30.0.10
- PostgreSQL: healthy no host 172.30.0.8
- Analytics/CDC/ClickHouse: healthy no host 172.30.0.9
- Bootstrap realtime de NFE corrigido para carregar stg.nfe diretamente em torqmind_current.stg_nfe_slim; parity validada com 70215 linhas
- Backfill current -> slim -> marts concluído com sucesso; NFE status=5 segregada corretamente em nfe_inutilizations_rt e fora de fraude/risco
- Root cause do freshness financeiro nesta rodada: STG financeiro seguia fresco, mas dw.fact_financeiro ficou parado em 2026-05-08; resolvido com purge scoped de 5450 linhas em dw.fact_financeiro para 2026-05-01..2026-05-12 e rerun do ETL full da mesma janela
- Consumer group torqmind-cdc-consumer-live encerrado em estado Stable com TOTAL-LAG=0
- Validacao multi-VM final: PASS em 2026-05-11 18:48 UTC com smoke de produto aprovado
- Proof pack final: tmp/prod-multivm-proof-20260511_184917.json com result=PASS
- Health publico e materialidade do produto: PASS em dashboard, sales, cash, fraud, goals, customers, finance e platform
- Pendencias bloqueantes neste ponto: nenhuma