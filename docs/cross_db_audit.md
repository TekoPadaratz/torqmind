# Auditoria Cruzada SQL Server ↔ PostgreSQL

Objetivo: localizar divergência entre a origem operacional Xpert/SQL Server e as camadas `STG`, `DW` e `MART` do TorqMind sem escrever em nenhum banco.

## Entradas via ambiente

- `AUDIT_PG_DSN`
- `AUDIT_SQLSERVER_DSN`
- `AUDIT_TENANT_ID`
- `AUDIT_BRANCH_IDS`
- `AUDIT_DATE_START`
- `AUDIT_DATE_END`
- `AUDIT_SAMPLE_DAYS`
- `AUDIT_OUTPUT_DIR`
- `AUDIT_FOCUSED_DAY` opcional
- `AUDIT_AGENT_CONFIG` opcional

Se `AUDIT_SQLSERVER_DSN` não for informado, o CLI tenta usar `apps/agent/config.local.yaml` ou o caminho passado em `AUDIT_AGENT_CONFIG`.

## Execução

```bash
cd apps/api
python -m app.cli.audit_sqlserver_vs_postgres \
  --tenant-id 1 \
  --branch-ids 14122,16305,18096 \
  --date-start 2026-03-01 \
  --date-end 2026-03-31 \
  --sample-days 5 \
  --output-dir /tmp/torqmind-audit
```

Modo focado em um dia:

```bash
cd apps/api
python -m app.cli.audit_sqlserver_vs_postgres \
  --tenant-id 1 \
  --branch-ids 14122 \
  --date-start 2026-03-24 \
  --date-end 2026-03-24 \
  --focused-day 2026-03-24 \
  --output-dir /tmp/torqmind-audit-20260324
```

## Artefatos gerados

- `audit_report.md`
- `audit_summary.json`
- `deltas_sales_by_day.csv`
- `deltas_sales_by_branch.csv`
- `deltas_payments_by_day.csv`
- `suspicious_documents.csv`
- `suspicious_items.csv`
- `tenant_branch_leak_checks.csv`
- `hypothesis_tests.md`

## Limitações conhecidas

- Materialized views não populadas/refrescadas são reportadas como indisponíveis; a auditoria não tenta corrigi-las.
- A comparação de UI/API é feita por semântica reproduzida em modo read-only; ela não chama endpoints HTTP.
- O utilitário não altera watermark, não faz reset e não executa ETL.

## Semântica auditada

- Vendas comerciais usam `movprodutos.situacao/status <> 2`.
- `situacao = 3` permanece venda válida para a trilha comercial.
- `comprovantes.cancelado` continua sendo verdade operacional para caixa, fraude e cancelamentos em turno.
- A auditoria não usa comprovante cancelado para excluir venda comercial.
