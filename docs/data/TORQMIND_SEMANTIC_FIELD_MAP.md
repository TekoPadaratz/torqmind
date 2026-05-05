# TorqMind Semantic Field Map

Data: 2026-05-04
Ambiente de referência: homolog oficial

## Regra de ouro

Quando houver nome novo e alias legado para o mesmo KPI, os dois valores devem sair preenchidos com a mesma semântica. Alias nulo com valor principal preenchido é regressão de contrato, não ausência real de dados.

## Caixa

| Conceito de produto | Campo canônico | Alias compatível | Origem operacional | Regra |
| --- | --- | --- | --- | --- |
| Recebimentos no período | `kpis.total_pagamentos` | `kpis.recebimentos_periodo` | `payments_by_type_rt` no realtime; DW/marts nos caminhos legados | soma do valor recebido no período selecionado |
| Cancelamentos no período | `kpis.total_cancelamentos` | `kpis.cancelamentos_periodo` | `sales_daily_rt.valor_cancelado` no realtime; DW/marts nos caminhos legados | soma monetária dos cancelamentos do período |
| Vendas no período | `kpis.total_vendas` | sem alias adicional | `sales_daily_rt.faturamento` ou marts equivalentes | total bruto de vendas ativas |
| Saldo comercial | `kpis.saldo_comercial` | sem alias adicional | derivado | `total_vendas - total_cancelamentos` |
| Mix de pagamentos | `historical.payment_mix[]` | `payment_breakdown[]` no realtime | `payments_by_type_rt` ou `mart.agg_pagamentos_diaria` | nunca inferir pelo DRE |

## Antifraude operacional

| Conceito de produto | Campo canônico | Origem | Regra |
| --- | --- | --- | --- |
| Cancelamentos operacionais | `kpis.cancelamentos` | `fraud_kpis()` | no realtime, `qtd_eventos` é exposto também, mas o campo de UI deve ser `cancelamentos` |
| Valor cancelado operacional | `kpis.valor_cancelado` | `fraud_kpis()` | no realtime, `impacto_total` é preservado como compatibilidade, mas a UI usa `valor_cancelado` |
| Série operacional | `by_day[].cancelamentos`, `by_day[].valor_cancelado` | `fraud_series()` | usar a leitura operacional reconciliada do período |
| Top operadores | `top_users[]` | `fraud_top_users()` | `cancelamentos` e `valor_cancelado` devem existir por linha |

## Antifraude modelado

| Conceito de produto | Campo canônico | Origem | Regra |
| --- | --- | --- | --- |
| Total de eventos de risco | `risk_kpis.total_eventos` | `risk_kpis()` | não confundir com `kpis.cancelamentos` |
| Impacto estimado total | `risk_kpis.impacto_total` | `risk_kpis()` | métrica modelada; não substitui o valor cancelado operacional |
| Score médio | `risk_kpis.score_medio` | `risk_kpis()` | escala de 0 a 100 |
| Cobertura do modelo | `model_coverage` | `risk_model_coverage()` | se parcial, a leitura operacional continua sendo a verdade do período |

## Filiais e labels críticos

| Campo | Origem preferencial | Fallback permitido | Não permitido |
| --- | --- | --- | --- |
| `payments_risk[].filial_nome` | `torqmind_current.stg_filiais` | string vazia apenas quando a filial realmente não existe | devolver vazio quando a filial existe em current |
| `payments_risk[].filial_label` | `_filial_label(id_filial, filial_nome)` | `Filial 123` quando o nome real não existir | `Filial sem cadastro` para filial já carregada em current |
| `turnos[].turno_label` | valor atual do turno em current/DW | `id_turno` | `Turno não identificado` quando o turno pode ser resolvido |

## Metas & Equipe

| Conceito de produto | Campo canônico | Origem | Regra |
| --- | --- | --- | --- |
| Ranking de equipe | `leaderboard[]` | vendas por funcionário | tela não deve depender de `goals_today` para ter materialidade |
| Risco por funcionário | `risk_top_employees[]` | engine de risco | complementar ao ranking, nunca substituto |
| Meta diária | `goals_today[]` | `app.goals` | ausência de linhas não implica tela vazia se ranking e projeção estiverem disponíveis |
| Projeção mensal | `monthly_projection.goal.target_value` | mistura controlada `app.goals` + série analítica | métrica válida mesmo com `goals_today` vazio |

## Financeiro

| Conceito de produto | Campo canônico | Origem | Regra |
| --- | --- | --- | --- |
| Receber em aberto | `kpis.receber_aberto` | aging financeiro | valor monetário aberto |
| Pagar em aberto | `kpis.pagar_aberto` | aging financeiro | valor monetário aberto |
| Evolução diária | `by_day[]` | visão financeira diária | deve ter materialidade para a tela principal |
| Pagamentos | `payments.kpis`, `payments.by_day`, `payments.by_turno`, `payments.anomalies` | domínio de pagamentos | não depender só de `aging` para considerar a tela saudável |

## Plataforma técnica

| Conceito de produto | Campo canônico | Origem | Regra |
| --- | --- | --- | --- |
| Saúde do streaming | `/platform/streaming-health` | `repos_mart_realtime.streaming_health()` | deve ser acessível apenas para papéis com acesso de plataforma |
| Freshness por fonte | `source_freshness[]` | health payload | zero linhas é aceitável apenas quando a fonte realmente não reporta dados |
| Erros recentes | `recent_errors[]` | health payload | lista vazia é saudável; erro 500 na rota é regressão crítica |

## Versionamento de snapshot nesta rodada

- `fraud_overview`: `contract_version=2`
- `cash_overview`: `contract_version=2`

Uso: quando um contrato quente muda e o snapshot antigo mascararia a correção, a assinatura deve ser versionada para forçar recomputação imediata.