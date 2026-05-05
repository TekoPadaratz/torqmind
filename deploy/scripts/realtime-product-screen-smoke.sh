#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BASE_URL="${BASE_URL:-http://localhost/api}"
API_CONTAINER="${API_CONTAINER:-torqmind-api-1}"
TENANT_ID="${TENANT_ID:-1}"
BRANCH_ID="${BRANCH_ID:--1}"
ROLE="${ROLE:-platform_master}"
SUBJECT="${SUBJECT:-ad519ee4-56c9-41fd-8ab0-9192a26e8d0a}"
WINDOW_DAYS="${WINDOW_DAYS:-30}"
DT_FIM="${DT_FIM:-$(date +%F)}"
DT_INI="${DT_INI:-$(date -d "${DT_FIM} -$((WINDOW_DAYS - 1)) days" +%F)}"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

generate_token() {
    docker exec "$API_CONTAINER" python -c "import sys; sys.path.insert(0, '/app'); from app.security import create_access_token; print(create_access_token({'sub': '${SUBJECT}', 'role': '${ROLE}', 'id_empresa': int('${TENANT_ID}'), 'id_filial': int('${BRANCH_ID}')}))"
}

if [[ -n "${TORQMIND_SMOKE_TOKEN:-}" ]]; then
  TOKEN="$TORQMIND_SMOKE_TOKEN"
else
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker não encontrado e TORQMIND_SMOKE_TOKEN não foi informado" >&2
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
    "sales sem KPIs ou sem produtos/série",
))

cash = load("cash")
cash_kpis = cash.get("kpis") or {}
cash_historical = (cash.get("historical") or {}).get("kpis") or {}
checks.append((
    "cash",
    cash_kpis.get("total_pagamentos") is not None and cash_kpis.get("recebimentos_periodo") is not None and cash_kpis.get("cancelamentos_periodo") is not None and (
        truthy_sequence((cash.get("historical") or {}).get("payment_mix")) or truthy_sequence(cash.get("turnos"))
    ),
    "cash sem aliases compatíveis ou sem mix/turnos",
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
    "goals sem leaderboard, risco ou projeção",
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
    "customers sem RFM ou sem blocos materiais de churn/delinquência",
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
    "platform sem payload mínimo de saúde técnica",
))

failed = [item for item in checks if not item[1]]

for name, ok, message in checks:
    prefix = "PASS" if ok else "FAIL"
    print(f"{prefix} {name}: {message if not ok else 'materialidade confirmada'}")

if failed:
    raise SystemExit(1)
PY

echo "Smoke concluído para ${BASE_URL} no período ${DT_INI}..${DT_FIM}."