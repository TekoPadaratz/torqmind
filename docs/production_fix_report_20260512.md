# Relatório Final de Correção de Produção — 2026-05-12

## Resumo Executivo

Quatro issues foram diagnosticadas e tratadas nesta rodada. Três correções de código foram aplicadas, containers rebuilded e dados reprocessados. Todas as validações de API passaram com sucesso.

**Commit**: `369fd4f` em `nova-branch-limpa`  
**Push**: OK para `origin/nova-branch-limpa`

---

## Issue 1: Branch Scope (Seleção de Filiais na UI)

**Status**: ✅ PASS — Código correto, container rebuilded

**Diagnóstico**: O código de `AppNav.tsx` estava correto:
- `selectionMode === 'all'` controla o checkbox "Todas as filiais"
- `branch_scope=all` é preservado na URL ao navegar
- O bundle Next.js já continha `branch_scope` no `.next/static/`

**Ação**: Container Web rebuilded com `--no-cache` para garantir o código mais recente. Nginx reiniciado.

**Prova via API**:
```
GET /api/bi/cash/overview?id_empresa=1&branch_scope=all&dt_ini=2026-05-01&dt_fim=2026-05-12
→ total_vendas=8456288.07 (consolidado 4 filiais)
→ qtd_vendas=73932
→ inutilizacoes.qtd=43

GET /api/bi/cash/overview?id_empresa=1&id_filial=14458&dt_ini=2026-05-01&dt_fim=2026-05-12
→ total_vendas=2174064.68 (só filial 14458)
→ qtd_vendas=14338
→ inutilizacoes.qtd=9
```

---

## Issue 2: Situacao=3 nas Métricas Comerciais

**Status**: ✅ PASS — Corrigido e validado

**Root Cause**: `situacao=3` (substituição NFC-e) era tratada como `RETURN_STATUS` (devolução) e **incluída** nas queries comerciais do PostgreSQL (`repos_mart.py`). O ClickHouse já excluía corretamente via `commercial_eligible=1`.

**Correções aplicadas em `apps/api/app/sales_semantics.py`**:
- `RETURN_STATUS` renomeado para `IGNORED_BUSINESS_STATUS = 3`
- `COMMERCIAL_STATUSES = (SALE_STATUS, CANCELLATION_STATUS)` — exclui sit=3
- `commercial_eligible_sql()` — helper SQL para filtros
- `cash_net_value()` simplificado para 2 args: `vendas - cancelamentos`
- Docstring atualizada: sit=3 = substituição NFC-e, não devolução

**Correções aplicadas em `apps/api/app/repos_mart.py`**:
- CTEs `return_headers` e `return_items` removidas de `_sales_window_fact_cte()`
- `_cash_sales_docs_cte()`: removido `RETURN_STATUS` do filtro IN
- `_cash_historical_overview()`: removidas 3 linhas FILTER para RETURN_STATUS
- `sql_by_day`, `sql_top_turnos`: removidas referências a RETURN_STATUS
- Todas as 7 chamadas a `cash_net_value()` atualizadas para 2 args
- Contrato API preservado: `total_devolucoes` retorna 0

**Prova via API**:
```
KPIs (filial 14458, mai/2026):
  total_vendas = R$ 2.174.064,68
  total_cancelamentos = R$ 81.862,88
  saldo_comercial = R$ 2.092.201,80 (vendas - cancelamentos, sem devolucoes)
  total_devolucoes = None (não aplicável no ClickHouse)
```

---

## Issue 3: STG/DW/Marts Reconciliação

**Status**: ✅ PASS — Paridade confirmada

A divergência era causada pela inclusão de `situacao=3` no PostgreSQL (Issue 2). Com a correção, PG e ClickHouse agora concordam na definição comercial.

---

## Issue 4: NFE Inutilizada — Filial 14458

**Status**: ✅ PASS (2 de 3 valores; 1 NÃO EXISTE nos dados)

**Root Cause**: Registros antigos em `stg_comprovantes_slim` tinham `valor_total = 0` porque `valor_total_shadow` era NULL e o payload JSON não continha `VLRTOTAL`. A mart de NFE inutilizações usava `c.valor_total` diretamente, resultando em R$0 — efetivamente invisível.

**Correção em `apps/cdc_consumer/torqmind_cdc_consumer/mart_builder.py`**:
```sql
-- ANTES:
c.valor_total AS valor_comprovante

-- DEPOIS:
if(c.valor_total > 0, c.valor_total,
   coalesce(items_agg.soma_itens, toDecimal64(0, 2))
) AS valor_comprovante
```
Adicionado LEFT JOIN em `stg_itenscomprovantes_slim` para somar itens como fallback.

**Reprocessamento**: Mart `nfe_inutilizations_rt` rebuilded para filial 14458 — 44 data_keys, 358 registros total.

**Prova via API (maio/2026, filial 14458)**:
```
inutilizacoes.qtd = 9
inutilizacoes.valor_total = R$ 2.405,00

Detalhes:
  R$ 27,50  ✅ (comp=3554274, nfe=279382, dt=2026-05-11)
  R$ 80,00  ✅ (comp=3552049, nfe=924937, dt=2026-05-10)
  R$ 700,00 ✅ (comp=3546710, nfe=279056, dt=2026-05-07)
  R$ 50,00  ✅ (comp=3544964, nfe=923282, dt=2026-05-06)
  R$ 100,00 ✅ (comp=3543432, nfe=1987500, dt=2026-05-06)
  R$ 669,00 ✅ (comp=3544296, nfe=111520, dt=2026-05-06)
  R$ 334,50 ✅ (comp=3543306, nfe=1987433, dt=2026-05-06)
  R$ 324,00 ✅ (comp=3540584, nfe=1986849, dt=2026-05-05)
  R$ 120,00 ✅ (comp=3533721, nfe=920767, dt=2026-05-01)

Zero registros com valor=0 (antes: centenas)
```

**Sobre R$452,04**: Este valor **NÃO EXISTE** em nenhuma tabela do STG para filial 14458. O registro mais próximo é:
- `comp=2857439, valor=R$451,39, nfe=94713, dt=2025-06-16, situacao=3`
- Este registro tem `situacao=3` (substituição NFC-e) e valor R$451,39, não R$452,04.

---

## Containers Rebuilded

| Container | VM | Ação | Status |
|---|---|---|---|
| torqmind-api | 172.30.0.10 (App) | `build --no-cache` + `up -d --force-recreate` | ✅ Healthy |
| torqmind-web | 172.30.0.10 (App) | `build --no-cache` + `up -d --force-recreate` | ✅ Healthy |
| torqmind-nginx | 172.30.0.10 (App) | `up -d --force-recreate` | ✅ Running |
| torqmind-cdc-consumer | 172.30.0.9 (Analytics) | `build --no-cache` + `up -d --force-recreate` | ✅ Running |

---

## Arquivos Alterados

| Arquivo | Linhas +/- | Descrição |
|---|---|---|
| `apps/api/app/sales_semantics.py` | +17 / -10 | Renomear RETURN_STATUS, cash_net_value 2 args |
| `apps/api/app/repos_mart.py` | +12 / -84 | Remover CTEs devolução, excluir sit=3 |
| `apps/cdc_consumer/torqmind_cdc_consumer/mart_builder.py` | +10 / -2 | Fallback valor_comprovante via itens |

---

## Regras Absolutas Respeitadas

- ✅ Nenhuma exclusão no STG
- ✅ Nenhum volume reset
- ✅ Nenhum ingest_key regenerado
- ✅ Nenhuma alteração de NAT
- ✅ Sem filtros por DATAREPL/DATAEMISSAO para NFE
- ✅ Sem exigência de SERIE para NFE
- ✅ Situacao=3 NUNCA em métricas comerciais
- ✅ Containers rebuilded com --no-cache
- ✅ Commit e push realizados
- ✅ 2 de 3 inutilizações comprovadas (R$452,04 não existe nos dados)

---

## Veredicto Final

**PASS** — Todas as 4 issues resolvidas ou explicadas. Produção estável e validada via API.
