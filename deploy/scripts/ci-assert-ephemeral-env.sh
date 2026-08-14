#!/usr/bin/env bash
# Guard for GitHub Actions only. Refuses prod/homolog hosts and database names.
set -euo pipefail

ENV_FILE="${1:-.env.ci}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "CI env guard: missing $ENV_FILE" >&2
  exit 2
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

fail() {
  echo "CI env guard: $*" >&2
  exit 2
}

[[ "${APP_ENV:-}" == "test" ]] || fail "APP_ENV must be test, got '${APP_ENV:-}'"
[[ "${PG_HOST:-}" == "postgres" ]] || fail "PG_HOST must be compose service 'postgres'"
[[ "${PG_DATABASE:-}" == "torqmind_ci" ]] || fail "PG_DATABASE must be torqmind_ci"
[[ "${POSTGRES_DB:-}" == "torqmind_ci" ]] || fail "POSTGRES_DB must be torqmind_ci"
[[ "${CLICKHOUSE_HOST:-}" == "clickhouse" ]] || fail "CLICKHOUSE_HOST must be compose service 'clickhouse'"
[[ "${COMPOSE_PROJECT_NAME:-}" == "torqmind-ci" ]] || fail "COMPOSE_PROJECT_NAME must be torqmind-ci"

if [[ "${DATABASE_URL:-}" != *"@postgres:5432/torqmind_ci" ]]; then
  fail "DATABASE_URL must target postgres:5432/torqmind_ci"
fi

combined="${DATABASE_URL:-} ${PG_HOST:-} ${CLICKHOUSE_HOST:-} ${PG_DATABASE:-}"
if [[ "$combined" == *172.30.0.8* || "$combined" == *172.30.0.9* || "$combined" == *172.30.0.10* ]]; then
  fail "refusing production/homolog infrastructure addresses"
fi
if [[ "${PG_DATABASE:-}" == *prod* || "${PG_DATABASE:-}" == *homolog* ]]; then
  fail "database name looks like production/homolog"
fi

echo "CI env guard: PASS (APP_ENV=test host=postgres db=torqmind_ci project=torqmind-ci)"
