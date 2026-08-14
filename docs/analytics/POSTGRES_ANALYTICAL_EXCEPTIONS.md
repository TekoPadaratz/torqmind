# Exceções PostgreSQL em telas analíticas

Contrato: ClickHouse-first. Registro normativo:

`apps/api/app/analytics_pg_exceptions.json`

A CI (`test_pg_analytical_exceptions_registry`) falha se:

- uma função `postgres_legacy` / `postgres_debt` / `postgres_app` não estiver no registry;
- uma dívida analítica nova entrar em `_CLICKHOUSE_DEBT_FUNCTIONS` sem o JSON;
- faltar campo obrigatório (função, endpoint, tela, fonte, motivo, responsável, risco, data, prazo, teste).

Não aplicar DDL ClickHouse sem autorização. Prazo das dívidas analíticas: 2026-09-15.
