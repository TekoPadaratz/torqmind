# Functional Audit

Data de auditoria: 2026-03-10

## Implementado e funcionando

| Módulo | UI | Endpoint | Repo/Serviço | Fonte | Status |
| --- | --- | --- | --- | --- | --- |
| Dashboard geral | `/dashboard` KPIs, evolução, radar, alerts, briefing | `/bi/dashboard/overview` | `repos_mart.dashboard_kpis`, `dashboard_series`, `payments_overview`, `risk_kpis`, `jarvis_briefing`, `notifications_unread_count` | `mart.agg_vendas_diaria`, `mart.agg_pagamentos_*`, `mart.agg_risco_diaria`, `app.notifications`, `app.insights_gerados` | Funcionando |
| Vendas | `/sales` | `/bi/sales/overview` | `dashboard_kpis`, `sales_by_hour`, `sales_top_products`, `sales_top_groups`, `sales_top_employees` | `mart.agg_vendas_*` | Funcionando |
| Anti-fraude | `/fraud` cancelamentos, risco, eventos, pagamentos | `/bi/fraud/overview` | `fraud_kpis`, `fraud_series`, `risk_kpis`, `risk_series`, `risk_top_employees`, `risk_by_turn_local`, `risk_last_events`, `payments_anomalies` | `mart.fraude_cancelamentos_*`, `dw.fact_risco_evento`, `mart.agg_risco_diaria`, `mart.risco_turno_local_diaria`, `mart.pagamentos_anomalias_diaria` | Funcionando |
| Clientes | `/customers` | `/bi/customers/overview`, `/bi/clients/churn`, `/bi/clients/retention-anonymous` | `customers_top`, `customers_rfm_snapshot`, `customers_churn_diamond`, `anonymous_retention_overview` | `dw.fact_venda`, `dw.dim_cliente`, marts de retenção/churn | Funcionando |
| Financeiro | `/finance` | `/bi/finance/overview`, `/bi/payments/overview` | `finance_kpis`, `finance_series`, `finance_aging_overview`, `payments_overview` | `mart.financeiro_*`, `mart.agg_pagamentos_*`, `mart.pagamentos_anomalias_diaria` | Funcionando |
| Metas/equipe | `/goals` | `/bi/goals/overview`, `/bi/risk/overview` | `leaderboard_employees`, `goals_today`, `risk_top_employees` | `mart.agg_vendas_*`, `app.employee_goals`, `mart.agg_risco_diaria` | Funcionando |
| Notifications | cards em `/dashboard` | `/bi/notifications`, `/bi/notifications/unread-count`, `/bi/notifications/{id}/read` | `notifications_list`, `notifications_unread_count`, `notification_mark_read` | `app.notifications` | Funcionando |
| Jarvis / insights | resumo IA e ações | `/bi/jarvis/briefing`, `/bi/jarvis/generate` | `jarvis_briefing`, `generate_jarvis_ai_plans` | `app.insights_gerados`, `app.insight_ai_cache` | Funcionando |

## Implementado, mas depende de fonte ou cobertura adicional

| Módulo | UI | Endpoint | Repo/Serviço | Fonte | Status |
| --- | --- | --- | --- | --- | --- |
| Caixa em aberto / turnos | cards em `/dashboard`, `/fraud`, `/finance` | embutido em `/bi/dashboard/overview`, `/bi/fraud/overview`, `/bi/finance/overview` | `repos_mart.open_cash_monitor` | `stg.turnos` | Implementado com fallback honesto. Hoje a fonte está vazia, então a UI mostra “dados de turno indisponíveis”. |
| Telegram crítico | backend / micro-risk / Jarvis | `/etl/micro_risk`, `/bi/admin/telegram/test`, geração de IA | `send_telegram_alert` | `app.telegram_settings`, `app.user_notification_settings` | Implementado, mas depende de configuração real de bot/chat_id para envio externo. |
| Competitor pricing | `/pricing` | `/bi/pricing/competitor/overview`, `/bi/pricing/competitor/prices` | `competitor_pricing_overview`, `competitor_pricing_upsert` | `app.competitor_fuel_prices` | Funcionando, mas requer filial e dados de preço concorrente para cenário completo. |

## Parcial

| Módulo | Gap | Impacto |
| --- | --- | --- |
| Caixa em aberto | Não há ETL/DW/MART nativo de turnos; o monitor atual opera direto sobre `stg.turnos` com mapeamento heurístico de payload. | Para demo está coberto com estado profissional. Para produção, precisa modelagem dedicada em DW/MART. |
| Goals | `goals_today` depende de metas cadastradas; sem metas, a tela mostra estado vazio profissional. | Sem comprometer demo, mas sem cadastro a narrativa de metas fica limitada. |
| Payments mapping | A tela mostra `% não categorizado`, mas não existe ainda um fluxo visual completo de mapeamento dentro do front. | Indicador existe; a operação corretiva ainda é parcial. |

## Não implementado

| Item | Situação |
| --- | --- |
| DW/MART dedicado para turnos/fechamento de caixa | Não implementado. Só existe `stg.turnos` e monitor operacional direto na STG. |
| Fonte real de turnos neste ambiente | Não disponível na carga atual. `stg.turnos` está vazia. |

## Observações de demo

- Anti-fraude está com dados reais materializados após o backfill otimizado.
- Componentes sem fonte agora devem exibir estado explícito, não tabela vazia.
- O recorte `2025-09-01` a `2025-09-30` neste ambiente possui dados até `2025-09-18` para risco materializado.
