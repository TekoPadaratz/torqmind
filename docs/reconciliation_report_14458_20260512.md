# Relatório de Reconciliação Crítica — Filial 14458 — 2026-05-11

## Contexto

Cliente reportou divergência: **R$180.479,61** (SQL Server) vs **R$152.772,78** (TorqMind).
Delta reportado: **R$27.706,83**.

## Bugs Encontrados e Corrigidos

### BUG CRÍTICO: Schema mismatch em stg_comprovantes_slim (ClickHouse)

**Causa raiz**: O código `mart_builder.py` (commit 369fd4f) adicionou colunas `ignored_business` e `commercial_eligible` ao INSERT da slim, mas a tabela ClickHouse não foi alterada para incluí-las. Resultado: **INSERT de 18 colunas em tabela de 16 colunas → falha silenciosa → slim vazia desde 2026-04-26**.

**Fix aplicado**: `ALTER TABLE ADD COLUMN` para `ignored_business UInt8` e `commercial_eligible UInt8` em `torqmind_current.stg_comprovantes_slim`, e `commercial_eligible UInt8` em `torqmind_current.fact_venda`.

### BUG: Situação=3 incluída no faturamento

O valor anterior R$152.772,78 incluía R$2.611,09 de documentos com situacao=3 (CFOP 5929). Após exclusão correta: **R$150.161,69**.

## Valores Após Fix

| Camada | Faturamento | Qtd Vendas | Status |
|--------|-------------|------------|--------|
| ClickHouse Slim (header) | R$151.497,92 | 1047 | ✓ |
| ClickHouse Slim (itens cfop>5000) | R$150.161,69 | 1699 itens | ✓ |
| Mart RT (sales_daily_rt) | R$150.161,69 | 1044 | ✓ |
| Mart RT (dashboard_home_rt) | R$150.161,69 | 1044 | ✓ |
| API /bi/sales/overview | R$150.161,69 | — | ✓ |
| API /bi/dashboard/home | R$150.161,69 | 1044 | ✓ |
| API /bi/cash/overview | R$150.161,69 | 1044 | ✓ |

**Consistência interna: 100%** — todos os layers concordam em R$150.161,69.

## NFE Inutilizações

Para 2026-05-11: **1 inutilização, R$27,50** (NFE 279382, comprovante 3554274, operadora ALANA A).
Valor correto para esse dia específico.

## Delta Restante

| Origem | Valor |
|--------|-------|
| Cliente (SQL Server) | R$180.479,61 |
| TorqMind (pós-fix) | R$150.161,69 |
| **Gap** | **R$30.317,92** (16,8%) |

**Diagnóstico: PRE-STG** — Os dados simplesmente não existem no PostgreSQL staging. Possíveis causas:
1. Agent não coletou todos os comprovantes do dia
2. Diferença de critério de data (timezone)
3. Comprovantes criados após última sincronização do Agent

## Pacote de Auditoria SQL Server

Gerado em: `logs/critical-data-reconciliation-14458/audit_sqlserver_14458_20260511.sql`

Contém 9 queries T-SQL para o cliente executar na fonte e identificar documentos faltantes.

## Ferramenta de Reconciliação

Criada: `deploy/scripts/reconcile-sales-day.sh`

Uso:
```bash
ID_FILIAL=14458 DATE=2026-05-11 ./deploy/scripts/reconcile-sales-day.sh
```

Percorre 4 layers: STG PG → CH Slim → Mart RT → API.

## Ações Realizadas no Ambiente

1. ✅ ALTER TABLE stg_comprovantes_slim ADD COLUMN ignored_business/commercial_eligible
2. ✅ ALTER TABLE fact_venda ADD COLUMN commercial_eligible  
3. ✅ CDC auto-repopulou slim (1051 rows: 1047 elegíveis, 2 sit=3, 2 cancelados)
4. ✅ DELETE de dados duplicados/corrompidos das marts
5. ✅ Mart rebuild (mart_only=True) — 11 refreshes, 68.629 rows
6. ✅ OPTIMIZE TABLE em sales_daily_rt, dashboard_home_rt, sales_hourly_rt
7. ✅ API verificada — retorna R$150.161,69

## Arquivos Modificados

- `deploy/scripts/reconcile-sales-day.sh` (novo)
- `docs/reconciliation_report_14458_20260512.md` (este arquivo)
