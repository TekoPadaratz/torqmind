# Privilege separation — TorqMind (Prompt 6)

**Status:** prepared only. **Do not apply** on Hom/Prod until ops review + TLS readiness.
**Does not** revoke or rotate the shared `torqmind` / ClickHouse god account.

## Why a single `POSTGRES_USER` is not enough

The API keeps **one logical pool family** keyed by `purpose` (`api|ingest|etl|migrate|auth`).
If every purpose resolves to the same DSN/user, `application_name` differs but **rights are identical**.
Separation requires **distinct role + DSN env** (`DATABASE_URL_INGEST`, `PG_USER_ETL`, …) **and** grants below.

## Consumers of today’s shared credentials (audit 2026-09-10)

| Consumer | Credential (logical) | Engine | Notes |
|---|---|---|---|
| API prod (`torqmind-api`) | PG `torqmind` / `DATABASE_URL` | PostgreSQL `.8` | superuser+BYPASSRLS observed |
| API homolog | same login pattern | PG (hom DB) | shared analytics CH with prod |
| CDC consumer `.9` | CH user (privileged) | ClickHouse | writes raw/current/ops/marts |
| Debezium `.9` | PG replication user/slot | PostgreSQL | **do not reset** slot/publication/offsets |
| ETL inside API process | same PG pool family | PostgreSQL | now `purpose=etl` |

Active CDC pipeline: analytics VM `.9` only. Residual Deb/RP on `.10` are empty — do not dual-run.

## Matrix (operations → purpose → schemas)

| Surface | App purpose | Schemas / objects | Min ops |
|---|---|---|---|
| Auth / MFA / users | `auth` | `auth.*`, `app.tenants` (read) | SELECT/UPDATE own tables; INSERT recovery codes |
| BI / platform OLTP | `api` | `app.*`, `auth.*` (session), read CH | DML `app`; SELECT needed auth |
| Ingest NDJSON | `ingest` | `stg.*` | INSERT/UPDATE STG only |
| ETL / publish mash | `etl` | `stg` read, `dw`/`mart` write, `etl.*` | EXECUTE refresh fns; DML dw/mart |
| Migrations CLI | `migrate` | all managed schemas | DDL owner / migrator role |
| CDC replication | (Debezium) | publication + slot | REPLICATION; SELECT published tables |
| CH BI read | API CH client | `torqmind_mart`, `torqmind_mart_rt` | SELECT |
| CH CDC write | CDC consumer | `torqmind_raw/current/ops` + marts | INSERT/ALTER as today → shrink later |
| CH publish from API | API CH client | mart tables used by `insert_batch` | INSERT |

Prod vs Hom: **separate role names** (`_hom` suffix) even if passwords differ; Hom must not use Prod role.

## TLS

- App now **preserves** `sslmode` / `sslrootcert` / … from `DATABASE_URL` and optional `PG_SSL*`.
- Defaults do **not** require TLS. Enabling `verify-full` needs CA on the **client image** and server certs — **infra dependency**.
- ClickHouse: `CLICKHOUSE_SECURE` / `CLICKHOUSE_VERIFY` / `CLICKHOUSE_CA_CERT` prepared; default secure=false.

## Rollout (gradual)

1. Create roles from templates (passwords via secrets manager / `psql` vars — **never** commit secrets).
2. Grant on Hom first; point Hom `DATABASE_URL_*` only.
3. Prove ingest + ETL + BI + login.
4. Prod: add roles alongside shared `torqmind` (do not revoke yet).
5. Flip purpose DSNs one service at a time.
6. Only after soak: remove excess grants from shared account (separate change window).

## Rollback

1. Point env back to shared `DATABASE_URL` / `torqmind`.
2. Redeploy API/CDC with previous env.
3. Leave new roles in place (inert) or DROP only after confirmed unused.
4. **Never** drop Debezium slot/publication to “fix” privileges.

## Destructive denial tests

Only on **ephemeral** CI/local Postgres (`TM_EPHEMERAL_LOCAL=1`). Never against Hom/Prod tables.
