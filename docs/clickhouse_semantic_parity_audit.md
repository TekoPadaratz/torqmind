# ClickHouse Semantic Parity Audit

## Summary

The ClickHouse migration made BI reads fast, but some marts lost the human labels that the PostgreSQL legacy path produced at query time. The fix keeps API reads simple by denormalizing those labels during DW sync, mart backfill, and incremental mart refresh.

Canonical sales origin remains `stg.comprovantes` and `stg.itenscomprovantes`. `stg.movprodutos` and `stg.itensmovprodutos` are not active sales sources.

## Screen Inventory

| Screen | Legacy source | ClickHouse source | Lost semantics found | Fix location |
| --- | --- | --- | --- | --- |
| Dashboard Geral | `mart.agg_*`, `dw.*`, scope helpers | `dashboard_home_bundle`, sales/cash/finance/risk marts | Freshness already fixed; relies on mart publication state | `repos_mart_clickhouse.py`, `torqmind_ops.sync_state` |
| Vendas | `mart.agg_vendas_*`, product/group/employee dims | `agg_vendas_diaria`, `agg_produtos_diaria`, `agg_grupos_diaria`, `agg_funcionarios_diaria` | Labels mostly preserved; frontend label already `Vendas normais` | Existing sales marts |
| Caixa | `dw.fact_caixa_turno`, `dw.fact_pagamento_comprovante`, `app.payment_type_map`, `dw.dim_usuario_caixa`, filiais | `agg_caixa_turno_aberto`, `agg_caixa_forma_pagamento`, payment marts | Payment labels became `FORMA_X`; DRE was unavailable; operator/branch fallbacks appeared when labels existed | `dim_forma_pagamento`, payment/cash marts, `cash_dre_summary` |
| Antifraude | `mart.fraude_cancelamentos_eventos`, filiais, caixa users | `fraude_cancelamentos_eventos`, `risco_eventos_recentes` | `filial_nome` and `usuario_nome` were blank in ClickHouse selects | Fraud/risk event marts and recent risk view |
| Clientes | Customer RFM/churn marts | `customer_rfm_daily`, `customer_churn_risk_daily`, `clientes_churn_risco` | No new semantic regression in this task | Existing customer marts |
| Financeiro | `dw.fact_financeiro`, finance marts | `financeiro_vencimentos_diaria`, `finance_aging_daily` | Cash DRE card was empty/unavailable; 1970 dates must never render | `cash_dre_summary` reads `finance_aging_daily` and returns `dt_ref=None` when absent |
| Preço Concorrente | `app.competitor_fuel_prices`, product dims, sales summary | PostgreSQL app flow remains authoritative for saved competitor prices | Snapshot could return stale overview after save; frontend cleared typed prices during refetch | Snapshot bypass for pricing overview; pricing page keeps inputs while refetching |
| Metas & Equipe | Goals app tables plus sales summary | Existing facade rules | No new semantic regression in this task | No change |

## Payment Labels

Source of truth for payment form names is `app.payment_type_map`.

ClickHouse now syncs it into `torqmind_dw.dim_forma_pagamento`:

- company-specific rows (`id_empresa`) win over global rows (`id_empresa IS NULL`);
- only `active=true` rows are used;
- marts store the chosen `label` and `category`;
- fallback is `Forma não identificada`, never `FORMA_X`.

Affected marts:

- `torqmind_mart.agg_pagamentos_diaria`
- `torqmind_mart.agg_pagamentos_turno`
- `torqmind_mart.agg_caixa_forma_pagamento`

## Branch And Operator Labels

ClickHouse marts now denormalize labels from:

- `torqmind_dw.dim_filial`
- `torqmind_dw.dim_usuario_caixa`
- `torqmind_dw.dim_funcionario`
- `torqmind_dw.dim_local_venda`

Affected objects:

- `torqmind_mart.fraude_cancelamentos_eventos`
- `torqmind_mart.risco_turno_local_diaria`
- `torqmind_mart.risco_eventos_recentes`

Operator precedence for cancellation/risk events is:

1. user on the event/comprovante when present;
2. user on the cash shift (`fact_caixa_turno`) when present;
3. explicit fallback `Operador não identificado`.

## Finance / DRE

`cash_dre_summary` now exists in the ClickHouse repository and uses `torqmind_mart.finance_aging_daily`.

Behavior:

- with a published finance snapshot, returns the same card keys expected by the frontend;
- without a finance snapshot, returns unavailable cards with `amount=null`;
- never converts missing dates/timestamps to `1970-01-01`.

## Competitor Pricing

Competitor price writes remain PostgreSQL app flow:

- `POST /bi/pricing/competitor/prices`
- `app.competitor_fuel_prices`

`pricing_competitor_overview` bypasses snapshot cache so a GET after POST reloads current saved values. The frontend keeps typed values during the refetch to avoid clearing the screen.

## Validation

Run:

```bash
ENV_FILE=/etc/torqmind/prod.env ID_EMPRESA=1 ID_FILIAL=14458 ./deploy/scripts/prod-semantic-marts-audit.sh
```

The audit fails on:

- `FORMA_*` labels in payment/cash marts;
- blank `filial_nome` or `usuario_nome` when a dimension row exists;
- 1970-like finance dates;
- missing finance mart when finance facts exist.

It warns on app-level objects that are absent but not critical to BI mart semantics.

## Remaining Debt

- `customers_delinquency_overview` still needs a customer-level finance/delinquency mart.
- `competitor_pricing_overview` remains PostgreSQL-owned for app writes and saved competitor prices; this is intentional.
- `monthly_goal_projection` still mixes app goals and analytical sales.
