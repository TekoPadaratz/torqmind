# Matriz de operações (Prompt 6) — revisão, não aplicada

| Rota / job | Conexão (`purpose`) | Destino | Operação mínima |
|---|---|---|---|
| `/auth/*`, MFA, security_attempts | `auth` | PG `auth.*` (+ `app.tenants` read) | DML auth |
| `/api` BI + platform OLTP | `api` | PG `app`/`auth`; CH mart read | DML app; SELECT CH |
| `/ingest/*` (não health) | `ingest` | PG `stg.*` | UPSERT STG |
| ETL orchestrator / publish | `etl` | PG stg→dw/mart/etl; CH insert | EXECUTE etl.*; DML dw/mart |
| `python -m app.cli.migrate` | `migrate` | schemas gerenciados | DDL |
| CDC consumer | CH write user | raw/current/ops/mart_rt | INSERT/ALTER |
| Debezium | replication role | publication/slot | REPLICATION + SELECT |

Identificação efetiva (sem segredo): `app.db.redacted_conn_summary(purpose)`.

Transição: ver `README.md` neste diretório. Rollback = voltar `DATABASE_URL` compartilhado; **não** dropar slot CDC.
