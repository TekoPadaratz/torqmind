# Queries de Reconciliação

Estas consultas existem para conferência operacional direta com o usuário.

## Regras semânticas antes de comparar

- Sempre use a mesma janela de negócio da tela.
- A data-base deve respeitar `BUSINESS_TIMEZONE` ou o override do tenant.
- Vendas operacionais:
  - usam `dw.fact_venda` com `cancelado = false`
  - `dw.fact_venda.cancelado` deve refletir `movprodutos.situacao/status = 2`
  - `movprodutos.situacao = 3` continua entrando em vendas
  - `comprovante.cancelado` não redefine a venda comercial
  - os itens usam `dw.fact_venda_item` com `cfop >= 5000`
- Caixa e cancelamentos operacionais:
  - usam `dw.fact_comprovante` com `cancelado = true`
  - exigem `id_turno IS NOT NULL`
  - exigem `CFOP > 5000`
- Antifraude operacional:
  - usa `mart.fraude_cancelamentos_eventos`
  - deve bater com os cancelamentos operacionais do mesmo recorte
- Antifraude modelado:
  - é outra leitura
  - depende da cobertura de `mart.agg_risco_diaria`
  - não deve ser misturado com ausência de evento operacional

## 1. Vendas

```sql
WITH sales_reconcile AS (
  SELECT
    COALESCE(SUM(total_venda), 0)::numeric(18,2) AS faturamento
  FROM dw.fact_venda
  WHERE id_empresa = :tenant_id
    AND id_filial = :branch_id
    AND data_key BETWEEN :dt_ini_key AND :dt_fim_key
    AND COALESCE(cancelado, false) = false
),
margin_reconcile AS (
  SELECT
    COALESCE(SUM(margem), 0)::numeric(18,2) AS margem,
    COALESCE(SUM(qtd), 0)::numeric(18,2) AS itens
  FROM dw.fact_venda_item
  WHERE id_empresa = :tenant_id
    AND id_filial = :branch_id
    AND data_key BETWEEN :dt_ini_key AND :dt_fim_key
    AND cfop >= 5000
)
SELECT *
FROM sales_reconcile
CROSS JOIN margin_reconcile;
```

Uso operacional:

- vendas do dia: `:dt_ini_key = :dt_fim_key = business_date`
- vendas ultimos 7 dias: use a mesma janela do filtro da tela
- o endpoint de referencia e `GET /bi/sales/overview`

## 2. Formas De Pagamento

```sql
WITH dw_payments AS (
  SELECT
    p.data_key,
    p.tipo_forma,
    COALESCE(SUM(p.valor), 0)::numeric(18,2) AS dw_total
  FROM dw.fact_pagamento_comprovante p
  WHERE p.id_empresa = :tenant_id
    AND (:branch_id IS NULL OR p.id_filial = :branch_id)
    AND p.data_key BETWEEN :dt_ini_key AND :dt_fim_key
  GROUP BY p.data_key, p.tipo_forma
),
mart_payments AS (
  SELECT
    m.data_key,
    m.tipo_forma,
    COALESCE(SUM(m.total_valor), 0)::numeric(18,2) AS mart_total
  FROM mart.agg_pagamentos_diaria m
  WHERE m.id_empresa = :tenant_id
    AND (:branch_id IS NULL OR m.id_filial = :branch_id)
    AND m.data_key BETWEEN :dt_ini_key AND :dt_fim_key
  GROUP BY m.data_key, m.tipo_forma
)
SELECT
  COALESCE(dw.data_key, mart.data_key) AS data_key,
  COALESCE(dw.tipo_forma, mart.tipo_forma) AS tipo_forma,
  COALESCE(dw.dw_total, 0)::numeric(18,2) AS dw_total,
  COALESCE(mart.mart_total, 0)::numeric(18,2) AS mart_total,
  (COALESCE(mart.mart_total, 0) - COALESCE(dw.dw_total, 0))::numeric(18,2) AS delta
FROM dw_payments dw
FULL OUTER JOIN mart_payments mart
  ON mart.data_key = dw.data_key
 AND mart.tipo_forma = dw.tipo_forma
ORDER BY data_key, tipo_forma;
```

Referencia funcional:

- `GET /bi/finance/overview`
- `GET /bi/payments/overview`
- `GET /bi/cash/overview`

## 3. Caixa

```sql
SELECT
  COUNT(*)::int AS qtd_cancelamentos,
  COALESCE(SUM(valor_total), 0)::numeric(18,2) AS total_cancelamentos
FROM dw.fact_comprovante
WHERE id_empresa = :tenant_id
  AND id_filial = :branch_id
  AND data_key BETWEEN :dt_ini_key AND :dt_fim_key
  AND COALESCE(cancelado, false) = true
  AND id_turno IS NOT NULL
  AND etl.safe_int(
        NULLIF(
          regexp_replace(COALESCE(payload->>'CFOP', ''), '[^0-9]', '', 'g'),
          ''
        )
      ) > 5000;
```

## 4. Antifraude Operacional

```sql
SELECT
  COUNT(*)::int AS cancelamentos,
  COALESCE(SUM(valor_total), 0)::numeric(18,2) AS valor_cancelado
FROM mart.fraude_cancelamentos_eventos
WHERE id_empresa = :tenant_id
  AND id_filial = :branch_id
  AND data_key BETWEEN :dt_ini_key AND :dt_fim_key;
```

## 5. Turnos E Caixa Aberto Agora

```sql
WITH dw_open AS (
  SELECT
    COUNT(*) FILTER (WHERE is_aberto = true)::int AS turnos_abertos_dw,
    COUNT(*)::int AS turnos_mapeados_dw
  FROM dw.fact_caixa_turno
  WHERE id_empresa = :tenant_id
    AND (:branch_id IS NULL OR id_filial = :branch_id)
),
mart_open AS (
  SELECT
    COUNT(*) FILTER (WHERE is_operational_live)::int AS turnos_abertos_mart,
    COUNT(*) FILTER (WHERE is_stale)::int AS turnos_stale_mart
  FROM mart.agg_caixa_turno_aberto
  WHERE id_empresa = :tenant_id
    AND (:branch_id IS NULL OR id_filial = :branch_id)
)
SELECT *
FROM dw_open
CROSS JOIN mart_open;
```

Referencia funcional:

- `GET /bi/cash/overview`
- `GET /bi/fraud/overview`

## 6. Top Clientes

```sql
WITH dw_top AS (
  SELECT
    v.id_cliente,
    COALESCE(SUM(i.total), 0)::numeric(18,2) AS dw_faturamento
  FROM dw.fact_venda v
  JOIN dw.fact_venda_item i
    ON i.id_empresa = v.id_empresa
   AND i.id_filial = v.id_filial
   AND i.id_db = v.id_db
   AND i.id_movprodutos = v.id_movprodutos
  WHERE v.id_empresa = :tenant_id
    AND (:branch_id IS NULL OR v.id_filial = :branch_id)
    AND v.data_key BETWEEN :dt_ini_key AND :dt_fim_key
    AND COALESCE(v.cancelado, false) = false
    AND v.id_cliente IS NOT NULL
    AND v.id_cliente <> -1
  GROUP BY v.id_cliente
),
mart_top AS (
  SELECT
    s.id_cliente,
    COALESCE(SUM(s.valor_dia), 0)::numeric(18,2) AS mart_faturamento
  FROM mart.customer_sales_daily s
  WHERE s.id_empresa = :tenant_id
    AND (:branch_id IS NULL OR s.id_filial = :branch_id)
    AND s.dt_ref BETWEEN :dt_ini::date AND :dt_fim::date
    AND s.id_cliente <> -1
  GROUP BY s.id_cliente
)
SELECT
  COALESCE(dw.id_cliente, mart.id_cliente) AS id_cliente,
  COALESCE(dw.dw_faturamento, 0)::numeric(18,2) AS dw_faturamento,
  COALESCE(mart.mart_faturamento, 0)::numeric(18,2) AS mart_faturamento,
  (COALESCE(mart.mart_faturamento, 0) - COALESCE(dw.dw_faturamento, 0))::numeric(18,2) AS delta
FROM dw_top dw
FULL OUTER JOIN mart_top mart
  ON mart.id_cliente = dw.id_cliente
ORDER BY GREATEST(COALESCE(dw.dw_faturamento, 0), COALESCE(mart.mart_faturamento, 0)) DESC
LIMIT 20;
```

Referencia funcional:

- `GET /bi/customers/overview`

## 7. Financeiro Basico

```sql
WITH dw_open AS (
  SELECT
    COALESCE(SUM(CASE WHEN f.tipo_titulo = 1 THEN GREATEST(0::numeric, COALESCE(f.valor,0) - COALESCE(f.valor_pago,0)) ELSE 0 END),0)::numeric(18,2) AS receber_aberto_dw,
    COALESCE(SUM(CASE WHEN f.tipo_titulo = 1 AND COALESCE(f.vencimento, f.data_emissao) < :as_of::date THEN GREATEST(0::numeric, COALESCE(f.valor,0) - COALESCE(f.valor_pago,0)) ELSE 0 END),0)::numeric(18,2) AS receber_vencido_dw,
    COALESCE(SUM(CASE WHEN f.tipo_titulo = 0 THEN GREATEST(0::numeric, COALESCE(f.valor,0) - COALESCE(f.valor_pago,0)) ELSE 0 END),0)::numeric(18,2) AS pagar_aberto_dw,
    COALESCE(SUM(CASE WHEN f.tipo_titulo = 0 AND COALESCE(f.vencimento, f.data_emissao) < :as_of::date THEN GREATEST(0::numeric, COALESCE(f.valor,0) - COALESCE(f.valor_pago,0)) ELSE 0 END),0)::numeric(18,2) AS pagar_vencido_dw
  FROM dw.fact_financeiro f
  WHERE f.id_empresa = :tenant_id
    AND (:branch_id IS NULL OR f.id_filial = :branch_id)
    AND COALESCE(f.vencimento, f.data_emissao) IS NOT NULL
    AND COALESCE(f.vencimento, f.data_emissao) <= :as_of::date
    AND (
      f.data_pagamento IS NULL
      OR f.data_pagamento > :as_of::date
      OR (COALESCE(f.valor,0) - COALESCE(f.valor_pago,0)) > 0
    )
),
mart_open AS (
  SELECT
    COALESCE(SUM(receber_total_aberto), 0)::numeric(18,2) AS receber_aberto_mart,
    COALESCE(SUM(receber_total_vencido), 0)::numeric(18,2) AS receber_vencido_mart,
    COALESCE(SUM(pagar_total_aberto), 0)::numeric(18,2) AS pagar_aberto_mart,
    COALESCE(SUM(pagar_total_vencido), 0)::numeric(18,2) AS pagar_vencido_mart
  FROM mart.finance_aging_daily
  WHERE id_empresa = :tenant_id
    AND (:branch_id IS NULL OR id_filial = :branch_id)
    AND dt_ref = :as_of::date
)
SELECT *
FROM dw_open
CROSS JOIN mart_open;
```

Referencia funcional:

- `GET /bi/finance/overview`

## 8. Cobertura Do Modelo De Risco

```sql
SELECT
  MIN(data_key)::int AS min_data_key,
  MAX(data_key)::int AS max_data_key,
  COUNT(*)::int AS rows
FROM mart.agg_risco_diaria
WHERE id_empresa = :tenant_id
  AND id_filial = :branch_id;
```

Interpretação:

- se a janela modelada não cobre o período inteiro, a leitura modelada deve ser tratada como parcial;
- mesmo assim, a leitura operacional do período continua válida para cancelamentos e eventos reais.

## 9. Impacto Estimado Do Modelo

- Cancelamento modelado: `impacto_estimado = valor_total * 0.70`
- Desconto alto: `impacto_estimado = GREATEST(desconto_total, valor_total * 0.08)`
- O valor é potencial exposto para priorização, não perda confirmada.

## 10. Validacao UI/API

Depois das reconciliacoes SQL, valide o mesmo recorte pelos endpoints:

- `GET /bi/dashboard/home`
- `GET /bi/sales/overview`
- `GET /bi/cash/overview`
- `GET /bi/finance/overview`
- `GET /bi/fraud/overview`
- `GET /bi/sync/status`

Checklist minimo:

- usar exatamente a mesma empresa, filial e janela das queries SQL
- validar timezone/data-base do negocio na resposta
- confirmar `reading_status`, `_snapshot_cache` e `business_clock`
- tratar `partial`, `value_gap` e `preparing` como estados reais, nao como erro cosmetico
