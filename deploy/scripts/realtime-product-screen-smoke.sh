#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.app.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.app.yml}"
PUBLIC_BASE_URL="${PRODUCT_SMOKE_BASE_URL:-http://127.0.0.1}"
PUBLIC_BASE_URL="${PUBLIC_BASE_URL%/}"
BASE_URL="${BASE_URL:-${PUBLIC_BASE_URL}/api}"
BASE_URL="${BASE_URL%/}"
API_CONTAINER="${API_CONTAINER:-torqmind-api}"
TENANT_ID="${TENANT_ID:-${PRODUCT_SMOKE_ID_EMPRESA:-1}}"
BRANCH_ID="${BRANCH_ID:--1}"
ROLE="${ROLE:-platform_master}"
SUBJECT="${SUBJECT:-ad519ee4-56c9-41fd-8ab0-9192a26e8d0a}"
WINDOW_DAYS="${WINDOW_DAYS:-30}"
DT_FIM="${DT_FIM:-${PRODUCT_SMOKE_DT_FIM:-$(date +%F)}}"
DT_INI="${DT_INI:-${PRODUCT_SMOKE_DT_INI:-$(date -d "${DT_FIM} -$((WINDOW_DAYS - 1)) days" +%F)}}"
PRODUCT_SMOKE_REQUIRE_MULTIVM="${PRODUCT_SMOKE_REQUIRE_MULTIVM:-false}"
PRODUCT_SMOKE_CHECK_PAGES="${PRODUCT_SMOKE_CHECK_PAGES:-auto}"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

HAS_COMPOSE_ENV=false
if [[ -f "$ENV_FILE" ]]; then
  HAS_COMPOSE_ENV=true
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

log() {
  printf '%s [product-smoke] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

compose_app() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

compose_service_running() {
  local service="$1"
  [[ "$HAS_COMPOSE_ENV" == "true" ]] || return 1
  command -v docker >/dev/null 2>&1 || return 1
  compose_app ps --status running --services 2>/dev/null | grep -Fx "$service" >/dev/null
}

require_http_page() {
  local name="$1"
  local url="$2"
  local body
  body="$(curl -fsS --max-time 20 "$url")"
  if [[ "$body" != *"__next"* && "$body" != *"TorqMind"* ]]; then
    echo "FAIL $name did not look like a rendered Next.js/TorqMind page" >&2
    return 1
  fi
  log "PASS $name page"
}

run_multivm_checks() {
  if [[ "$PRODUCT_SMOKE_REQUIRE_MULTIVM" != "true" && "$PRODUCT_SMOKE_REQUIRE_MULTIVM" != "1" ]]; then
    return 0
  fi
  if [[ "$HAS_COMPOSE_ENV" != "true" ]]; then
    echo "ENV_FILE=$ENV_FILE not found; required for multi-VM smoke" >&2
    exit 2
  fi

  log "Checking App compose services"
  compose_service_running api
  compose_service_running web
  compose_service_running nginx

  log "Checking API effective multi-VM flags"
  compose_app exec -T api python - <<'PY'
from app.config import settings

assert settings.realtime_marts_fallback is False, "REALTIME_MARTS_FALLBACK must be false"
assert settings.pg_host not in {"postgres", "localhost", "127.0.0.1"}, "PG_HOST must be remote in multi-VM"
assert settings.clickhouse_host not in {"clickhouse", "localhost", "127.0.0.1"}, "CLICKHOUSE_HOST must be remote in multi-VM"
print("API_FLAGS_OK")
PY
}

run_public_page_checks() {
  local should_check="$PRODUCT_SMOKE_CHECK_PAGES"
  if [[ "$should_check" == "auto" ]]; then
    should_check=false
    if [[ "$PRODUCT_SMOKE_REQUIRE_MULTIVM" == "true" || "$PRODUCT_SMOKE_REQUIRE_MULTIVM" == "1" ]]; then
      should_check=true
    fi
  fi
  [[ "$should_check" == "true" || "$should_check" == "1" ]] || return 0

  log "Checking public product pages through Nginx"
  require_http_page login "$PUBLIC_BASE_URL/"
  require_http_page dashboard "$PUBLIC_BASE_URL/dashboard?dt_ini=$DT_INI&dt_fim=$DT_FIM&id_empresa=$TENANT_ID&scope_epoch=prod-smoke"
  require_http_page sales "$PUBLIC_BASE_URL/sales?dt_ini=$DT_INI&dt_fim=$DT_FIM&id_empresa=$TENANT_ID&scope_epoch=prod-smoke"
}

generate_token_in_compose() {
  compose_app exec -T api env \
    TOKEN_SUBJECT="$SUBJECT" \
    TOKEN_ROLE="$ROLE" \
    TOKEN_TENANT_ID="$TENANT_ID" \
    TOKEN_BRANCH_ID="$BRANCH_ID" \
    python - <<'PY'
import os
from app.security import create_access_token

print(create_access_token({
    "sub": os.environ["TOKEN_SUBJECT"],
    "role": os.environ["TOKEN_ROLE"],
    "id_empresa": int(os.environ["TOKEN_TENANT_ID"]),
    "id_filial": int(os.environ["TOKEN_BRANCH_ID"]),
}))
PY
}

generate_token_in_container() {
  local container="$1"
  docker exec \
    -e TOKEN_SUBJECT="$SUBJECT" \
    -e TOKEN_ROLE="$ROLE" \
    -e TOKEN_TENANT_ID="$TENANT_ID" \
    -e TOKEN_BRANCH_ID="$BRANCH_ID" \
    "$container" \
    python - <<'PY'
import os
from app.security import create_access_token

print(create_access_token({
    "sub": os.environ["TOKEN_SUBJECT"],
    "role": os.environ["TOKEN_ROLE"],
    "id_empresa": int(os.environ["TOKEN_TENANT_ID"]),
    "id_filial": int(os.environ["TOKEN_BRANCH_ID"]),
}))
PY
}

generate_token() {
  if compose_service_running api; then
    generate_token_in_compose
    return
  fi
  if generate_token_in_container "$API_CONTAINER" 2>/dev/null; then
    return
  fi
  generate_token_in_container torqmind-api-1
}

run_multivm_checks
run_public_page_checks

if [[ -n "${TORQMIND_SMOKE_TOKEN:-}" ]]; then
  TOKEN="$TORQMIND_SMOKE_TOKEN"
else
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker not found and TORQMIND_SMOKE_TOKEN was not provided" >&2
    exit 2
  fi
  TOKEN="$(generate_token)"
fi

fetch_json() {
  local name="$1"
  local path="$2"
  local status

  status="$(curl -sS -o "$TMP_DIR/$name.json" -w '%{http_code}' "$BASE_URL$path" -H "Authorization: Bearer $TOKEN")"
  printf '%s' "$status" > "$TMP_DIR/$name.status"
  if [[ "$status" != "200" ]]; then
    echo "FAIL $name HTTP $status" >&2
    return 1
  fi
}

fetch_json health "/health"
fetch_json dashboard "/bi/dashboard/home?dt_ini=${DT_INI}&dt_fim=${DT_FIM}&id_empresa=${TENANT_ID}"
fetch_json sales "/bi/sales/overview?dt_ini=${DT_INI}&dt_fim=${DT_FIM}&id_empresa=${TENANT_ID}"
fetch_json cash "/bi/cash/overview?dt_ini=${DT_INI}&dt_fim=${DT_FIM}&id_empresa=${TENANT_ID}"
fetch_json fraud "/bi/fraud/overview?dt_ini=${DT_INI}&dt_fim=${DT_FIM}&id_empresa=${TENANT_ID}"
fetch_json goals "/bi/goals/overview?dt_ini=${DT_INI}&dt_fim=${DT_FIM}&id_empresa=${TENANT_ID}"
fetch_json customers "/bi/customers/overview?dt_ini=${DT_INI}&dt_fim=${DT_FIM}&id_empresa=${TENANT_ID}"
fetch_json finance "/bi/finance/overview?dt_ini=${DT_INI}&dt_fim=${DT_FIM}&id_empresa=${TENANT_ID}"
fetch_json platform "/platform/streaming-health"

python3 - "$TMP_DIR" <<'PY'
import json
import sys
from pathlib import Path

tmp_dir = Path(sys.argv[1])


def load(name: str):
    return json.loads((tmp_dir / f"{name}.json").read_text(encoding="utf-8"))


def truthy_sequence(value):
    return isinstance(value, list) and len(value) > 0


def truthy_mapping(value):
    return isinstance(value, dict) and len(value) > 0


checks = []

health = load("health")
checks.append(("health", bool(health.get("ok")), "health sem ok=true"))

dashboard = load("dashboard")
dashboard_sales = (dashboard.get("overview") or {}).get("sales") or {}
checks.append((
    "dashboard",
    truthy_mapping(dashboard.get("kpis")) and (truthy_sequence(dashboard_sales.get("top_products")) or truthy_sequence(dashboard_sales.get("by_day"))),
    "dashboard sem KPIs ou sem materialidade comercial",
))

sales = load("sales")
checks.append((
    "sales",
    truthy_mapping(sales.get("kpis")) and (truthy_sequence(sales.get("top_products")) or truthy_sequence(sales.get("by_day"))),
    "sales sem KPIs ou sem produtos/serie",
))

cash = load("cash")
cash_kpis = cash.get("kpis") or {}
checks.append((
    "cash",
    cash_kpis.get("total_pagamentos") is not None and cash_kpis.get("recebimentos_periodo") is not None and cash_kpis.get("cancelamentos_periodo") is not None and (
        truthy_sequence((cash.get("historical") or {}).get("payment_mix")) or truthy_sequence(cash.get("turnos"))
    ),
    "cash sem aliases compativeis ou sem mix/turnos",
))

fraud = load("fraud")
payments_risk = fraud.get("payments_risk") or []
payments_label_ok = True
if payments_risk:
    payments_label_ok = all(str(item.get("filial_label") or "").strip() not in {"", "Filial sem cadastro"} for item in payments_risk[:5])
checks.append((
    "fraud",
    (fraud.get("kpis") or {}).get("cancelamentos") is not None and (fraud.get("kpis") or {}).get("valor_cancelado") is not None and (
        truthy_sequence(fraud.get("top_users")) or truthy_sequence(fraud.get("last_events")) or truthy_sequence(payments_risk)
    ) and payments_label_ok,
    "fraud sem KPIs operacionais ou com labels de filial degradados",
))

goals = load("goals")
projection = goals.get("monthly_projection") or {}
projection_goal = ((projection.get("goal") or {}).get("target_value"))
checks.append((
    "goals",
    truthy_sequence(goals.get("leaderboard")) or truthy_sequence(goals.get("risk_top_employees")) or projection_goal is not None,
    "goals sem leaderboard, risco ou projecao",
))

customers = load("customers")
anon = customers.get("anonymous_retention") or {}
delinquency = customers.get("delinquency") or {}
checks.append((
    "customers",
    truthy_mapping(customers.get("rfm")) and (
        truthy_sequence(customers.get("top_customers"))
        or truthy_sequence(customers.get("churn_top"))
        or truthy_sequence(anon.get("breakdown_dow"))
        or truthy_sequence(delinquency.get("buckets"))
        or truthy_sequence(delinquency.get("customers"))
    ),
    "customers sem RFM ou sem blocos materiais de churn/delinquencia",
))

finance = load("finance")
payments = finance.get("payments") or {}
checks.append((
    "finance",
    truthy_mapping(finance.get("kpis")) and (
        truthy_sequence(finance.get("by_day"))
        or truthy_mapping(finance.get("aging"))
        or truthy_sequence(payments.get("by_day"))
        or truthy_sequence(payments.get("anomalies"))
    ),
    "finance sem KPIs ou sem aging/pagamentos materiais",
))

platform = load("platform")
checks.append((
    "platform",
    truthy_mapping(platform) and "use_realtime_marts" in platform and "source_freshness" in platform and "recent_errors" in platform,
    "platform sem payload minimo de saude tecnica",
))

failed = [item for item in checks if not item[1]]

for name, ok, message in checks:
    prefix = "PASS" if ok else "FAIL"
    print(f"{prefix} {name}: {message if not ok else 'materialidade confirmada'}")

if failed:
    raise SystemExit(1)
PY

echo "Smoke concluido para ${BASE_URL} no periodo ${DT_INI}..${DT_FIM}."
