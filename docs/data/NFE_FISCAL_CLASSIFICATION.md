# NFE Fiscal Classification

## Overview

TorqMind classifies fiscal documents (NFE) based on their `status` field from the SQL Server `dbo.NFE` table to correctly distinguish between real cancellations, inutilized documents, and authorized sales.

## Status Values

| Status | Label         | Meaning                                                |
|--------|---------------|--------------------------------------------------------|
| 3      | Autorizada    | NFE authorized — normal sale, no special treatment     |
| 4      | Cancelada     | NFE cancelled — real fiscal cancellation               |
| 5      | Inutilizada   | NFE voided/inutilized — NOT a real cancellation        |

## Classification Rules

Given a `comprovante` with `cancelado=1` (cancelled flag in the POS system):

| Condition                           | Classification           | In Fraud? | In Cancellation Count? |
|-------------------------------------|--------------------------|-----------|------------------------|
| `cancelado=0`                       | `sale_active`            | No        | No                     |
| `cancelado=1` + NFE `status=4`     | `cancellation_real`      | Yes       | Yes                    |
| `cancelado=1` + NFE `status=5`     | `nfe_inutilized`         | **No**    | **No**                 |
| `cancelado=1` + no NFE record      | `cancellation_without_nfe` | Yes     | Yes                    |

## Impact on Dashboards

### Fraud / Antifraude
- NFE status=5 records are **excluded** from fraud KPIs, fraud events, and risk calculations
- Only `cancelado=1` with `status != 5` (or no NFE) count as potential fraud

### Sales / Vendas
- Cancellation totals (`qtd_canceladas`, `valor_cancelado`) exclude NFE status=5
- Active sales remain unaffected

### Dashboard Home
- Same exclusion as Sales for the cancel_agg subquery

### Caixa (Cash)
- A new "Notas Fiscais Inutilizadas" section shows NFE status=5 documents separately
- KPIs: count and total value of inutilized documents
- Detail table: filial, turno, operator, NFE number, series, value, date

## Data Pipeline

### Agent → API Ingest
- Agent extracts from `dbo.NFE` with watermark on `DATAREPL`
- Sends NDJSON to `POST /ingest/nfe`
- Shadow columns: `status_shadow`, `numero_nfe_shadow`, `serie_shadow`, `chave_nfe_shadow`, `modelo_shadow`, `protocolo_shadow`, `data_emissao_shadow`, `data_autorizacao_shadow`, `data_cancelamento_shadow`, `data_inutilizacao_shadow`, `valor_nfe_shadow`

### PostgreSQL STG
- Table: `stg.nfe` (migration `074_stg_nfe.sql`)
- PK: `(id_empresa, id_filial, id_db, id_comprovante, id_nfe)`

### ClickHouse Slim
- Table: `torqmind_current.stg_nfe_slim` (DDL `026_slim_nfe.sql`)
- ReplacingMergeTree ordered by `(id_empresa, id_filial, id_db, id_comprovante, id_nfe)`
- Typed columns: `status`, `numero_nfe`, `serie`, `chave_nfe`, `modelo`, `data_emissao`, `valor_nfe`

### ClickHouse Mart
- Table: `torqmind_mart_rt.nfe_inutilizations_rt` (DDL `042_mart_nfe_inutilizations.sql`)
- JOINs comprovantes_slim + nfe_slim + turnos + usuarios + filiais
- Only includes records where `cancelado=1 AND nfe_status=5`

### MartBuilder Integration
- `_populate_slim_nfe()`: Extracts typed fields from STG payload into slim table
- `_nfe_latest_status_cte()`: CTE helper for latest NFE status per comprovante
- Existing marts modified: `fraud_daily_rt`, `risk_recent_events_rt`, `sales_daily_rt`, `dashboard_home_rt`
- New mart: `nfe_inutilizations_rt`

## Graceful Degradation

All NFE-aware queries check if `stg_nfe_slim` exists before attempting JOINs. If NFE data hasn't been ingested yet, the system falls back to the original behavior (all `cancelado=1` treated as cancellations).

## Bootstrap

To backfill NFE historical data:
1. Apply PostgreSQL migration: `074_stg_nfe.sql`
2. Agent will ingest NFE records automatically (365-day bootstrap window)
3. Run `realtime-bootstrap-stg.sh` to copy to ClickHouse if needed
4. Mart backfill will process NFE classification automatically
