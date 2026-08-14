#!/usr/bin/env bash
# Guard for GitHub Actions / hermetic CI only. Refuses prod/homolog hosts and database names.
set -euo pipefail

ENV_FILE="${1:-}"
if [[ -n "${ENV_FILE}" ]]; then
  if [[ ! -f "${ENV_FILE}" ]]; then
    echo "CI env guard: missing ${ENV_FILE}" >&2
    exit 2
  fi
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

fail() {
  echo "CI env guard: $*" >&2
  exit 2
}

host="${PG_HOST:-}"
db="${PG_DATABASE:-${POSTGRES_DB:-}}"
url="${DATABASE_URL:-}"

[[ "${APP_ENV:-}" == "test" ]] || fail "APP_ENV must be test, got '${APP_ENV:-}'"
[[ "${db}" == "torqmind_ci" ]] || fail "PG_DATABASE/POSTGRES_DB must be torqmind_ci, got '${db}'"

case "${host}" in
  postgres|localhost|127.0.0.1) ;;
  *) fail "PG_HOST must be postgres (compose) or localhost/127.0.0.1 (GHA), got '${host}'" ;;
esac

if [[ "${url}" == *"172.30.0."* || "${host}" == *"172.30.0."* ]]; then
  fail "refusing production/homolog infrastructure addresses"
fi
if [[ "${url}" == *"@postgres:"* ]]; then
  [[ "${url}" == *"@postgres:"*"torqmind_ci" ]] || fail "DATABASE_URL must target postgres/.../torqmind_ci"
elif [[ -n "${url}" ]]; then
  [[ "${url}" == *"/torqmind_ci" ]] || fail "DATABASE_URL must target database torqmind_ci"
  [[ "${url}" == *"@127.0.0.1:"* || "${url}" == *"@localhost:"* ]] || fail "DATABASE_URL host must be localhost or 127.0.0.1"
fi

if [[ "${db}" == *prod* || "${db}" == *homolog* ]]; then
  fail "database name looks like production/homolog"
fi

echo "CI env guard: PASS (APP_ENV=test host=${host} db=${db})"
