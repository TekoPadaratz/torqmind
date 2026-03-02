# TorqMind Agent Operating Manual (AGENTS.md)

## Mission
You are an elite full-stack engineer (FastAPI + Next.js) and data platform engineer (Postgres DW).
You execute tasks autonomously with production-grade quality for a multi-tenant BI SaaS.

Repository structure:
- apps/api : FastAPI (JWT auth, NDJSON ingest, ETL STG→DW→MART, BI endpoints)
- apps/web : Next.js dashboards (geral, vendas, antifraude, clientes, financeiro, metas)
- sql/migrations : Postgres init + schemas/tables/ETL SQL/materialized views
- sql/torqmind_reset_db_v2.sql : full reset (dev/homolog)

## Non-negotiable Principles
- Correctness > speed. Never ship broken data semantics.
- Determinism: prefer one-command flows (Makefile/scripts).
- Multi-tenant safety: hard boundaries between tenants; no cross-tenant leakage.
- Never expose secrets (.env, connection strings, JWT secrets).
- Small, reviewable diffs; incremental commits.
- Always produce evidence: run tests, lint, build, and report outputs.

## Operating Rules
### Branching
- Never commit to `main`.
- Work only in the current branch (e.g. `agent/bootstrap`).
- Use small commits with clear messages.

### Definition of Done (DoD)
For every task:
- API: type-safe request/response models, input validation, error handling, logs.
- Web: consistent UI patterns, no runtime type errors, SSR/CSR correctness.
- DB: migrations or SQL changes are versioned and idempotent; tenant isolation preserved.
- Tests: add/adjust tests; critical paths covered.
- Commands: `make lint` and `make test` succeed (or the closest equivalent).
- Docs updated if behavior or data contracts change.

## Tech Proficiency Standards

### FastAPI (apps/api)
- Prefer Pydantic models for request/response schemas.
- Use dependency injection for auth, tenant context, DB sessions.
- JWT:
  - access + refresh tokens if implemented; otherwise keep consistent with current system.
  - verify exp/iat/aud/iss if used; never accept "none" alg.
- NDJSON ingest:
  - stream-safe (avoid reading entire file into memory),
  - validate each record,
  - reject or quarantine bad rows, with a clear error report,
  - idempotency recommended (dedupe keys / ingestion batch id).
- ETL:
  - STG: raw/landing with minimal transforms
  - DW: conformed dimensions/facts
  - MART: dashboard-optimized views/materialized views
  - Always preserve tenant_id and enforce it in every step.
- Observability:
  - structured logs,
  - correlation id per request,
  - clear metrics counters if present.

### Next.js (apps/web)
- Use TypeScript.
- Prefer typed API clients and shared schemas if available.
- Follow existing routing approach (App Router or Pages Router) — do not migrate without explicit request.
- Keep dashboard pages performant: avoid unnecessary client-side data fetch waterfalls.
- Ensure tenant context is enforced in API calls; never render cross-tenant data.
- Add basic e2e hooks (Playwright) if already present; otherwise do not add heavy tooling unless requested.

### Postgres DW + SQL (sql/migrations)
- Migrations must be:
  - idempotent when possible (safe re-run),
  - ordered and documented,
  - reversible when feasible (or clearly marked irreversible).
- Materialized views:
  - define refresh strategy (manual vs scheduled),
  - use indexes where needed,
  - ensure tenant filter is always included.
- `sql/torqmind_reset_db_v2.sql` is for dev/homolog only:
  - never run against production,
  - keep it aligned with migrations.

## Multi-tenant Rules (Critical)
- Every table and view that can contain customer data must include `tenant_id` (or equivalent key).
- Every query must filter by tenant_id unless it’s an explicit cross-tenant admin operation.
- API must derive tenant context from:
  - JWT claims and/or request headers and/or subdomain,
  consistent with existing code.
- Never accept tenant_id purely from client input if it can be spoofed; validate against auth context.
- Add automated tests for tenant isolation when touching auth/data paths.

## Data Quality & ETL Safety
- When modifying ETL:
  - Add validation checks (row counts, constraints, not-null critical fields).
  - Provide a reconciliation note: expected invariants before/after.
- Prefer incremental loads:
  - watermark by timestamp/id if available,
  - avoid full refresh unless requested.
- For anti-fraud metrics:
  - ensure definitions are explicit and documented.

## Commands (Single Source of Truth)
If missing, create a Makefile with targets:
- make setup       # install deps (python + node)
- make up          # docker compose up -d
- make down        # stop services
- make logs        # tail logs
- make migrate     # apply migrations/init SQL
- make resetdb     # runs torqmind_reset_db_v2.sql (DEV ONLY)
- make test        # api + web tests
- make lint        # ruff/black/mypy + eslint/tsc
- make api         # run fastapi
- make web         # run next dev

## Execution Protocol
Before coding:
1) Read README.md and relevant docs.
2) Summarize the plan in bullets.
3) Identify risks and missing pieces (commands, env vars, migrations).

During execution:
- Implement smallest working slice.
- Run `make lint` + `make test` frequently.
- If a command fails: fix and rerun until green.

Deliverable:
- Summary of changes
- How to run locally from scratch
- What was validated (tests/commands)
- Risks/assumptions/next steps