#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
RUN_ETL="${RUN_ETL:-0}"

# shellcheck source=deploy/scripts/lib/prod-env.sh
source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

tm_require_prod_runtime_env "$ENV_FILE"

echo "== systemd =="
systemctl is-enabled docker
systemctl is-active docker
systemctl is-enabled cron
systemctl is-active cron

echo
echo "== cron =="
crontab -l | grep -F "TorqMind ETL schedule"

echo
echo "== containers =="
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" ps

echo
echo "== smoke =="
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T api env RUN_ETL="$RUN_ETL" python - <<'PY'
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import timedelta

from app.business_time import business_today
from app.db import get_conn


API_BASE = "http://127.0.0.1:8000"
PUBLIC_BASE = "http://nginx"


def request_json(method: str, base: str, path: str, *, payload: dict | None = None, headers: dict[str, str] | None = None):
    request_headers = {"Accept": "application/json"}
    if headers:
        request_headers.update(headers)
    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path, method=method, data=body, headers=request_headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            raw = response.read().decode("utf-8")
            return response.status, json.loads(raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {"raw": raw}
        return exc.code, body


def request_ingest_dry_run(ingest_key: str):
    req = urllib.request.Request(
        API_BASE + "/ingest/filiais",
        method="POST",
        data=b"",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-ndjson",
            "X-Ingest-Key": ingest_key,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            raw = response.read().decode("utf-8")
            return response.status, json.loads(raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {"raw": raw}
        return exc.code, body


def ensure_keys(body: dict, *keys: str) -> None:
    missing = [key for key in keys if key not in body]
    if missing:
        raise RuntimeError(f"Missing key(s) {missing} in response: {body}")


def ensure_module_ready(module_name: str, body: dict) -> None:
    data_state = str(body.get("data_state") or "").strip().lower()
    reading_status = str(body.get("reading_status") or "").strip().lower()
    snapshot_meta = body.get("_snapshot_cache") if isinstance(body.get("_snapshot_cache"), dict) else {}
    snapshot_source = str(snapshot_meta.get("source") or "").strip().lower()
    snapshot_mode = str(snapshot_meta.get("mode") or "").strip().lower()
    snapshot_reason = str(snapshot_meta.get("reason") or "").strip()

    if data_state == "transient_unavailable":
        raise RuntimeError(
            f"{module_name} returned transient_unavailable during smoke: "
            f"source={snapshot_source or 'n/a'} mode={snapshot_mode or 'n/a'} reason={snapshot_reason or 'n/a'}"
        )
    if reading_status == "preparing":
        raise RuntimeError(
            f"{module_name} is still preparing during smoke: "
            f"source={snapshot_source or 'n/a'} mode={snapshot_mode or 'n/a'} reason={snapshot_reason or 'n/a'}"
        )
    if snapshot_mode in {"protected_unavailable", "warming_up", "live_unavailable"}:
        raise RuntimeError(
            f"{module_name} reported unavailable snapshot/live mode during smoke: "
            f"source={snapshot_source or 'n/a'} mode={snapshot_mode} reason={snapshot_reason or 'n/a'}"
        )


def build_scope() -> tuple[int, int | None, str, str]:
    tenant_id = None
    branch_id = None
    dt_fim = None
    dt_ini = None

    if me_body.get("default_scope"):
        default_scope = me_body["default_scope"]
        tenant_id = default_scope.get("id_empresa")
        branch_id = default_scope.get("id_filial")
        dt_ini = default_scope.get("dt_ini")
        dt_fim = default_scope.get("dt_fim")

    if tenant_id is None:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            tenant_row = conn.execute(
                """
                SELECT id_empresa
                FROM app.tenants
                WHERE is_active = true
                ORDER BY id_empresa
                LIMIT 1
                """
            ).fetchone()
            if not tenant_row:
                raise RuntimeError("No active tenant found for release smoke.")
            tenant_id = int(tenant_row["id_empresa"])
            branch_row = conn.execute(
                """
                SELECT id_filial
                FROM auth.filiais
                WHERE id_empresa = %s
                  AND is_active = true
                ORDER BY id_filial
                LIMIT 1
                """,
                (tenant_id,),
            ).fetchone()
            branch_id = int(branch_row["id_filial"]) if branch_row and branch_row["id_filial"] is not None else None

    if dt_fim is None:
        dt_fim = business_today(int(tenant_id)).isoformat()
    if dt_ini is None:
        dt_ini = (business_today(int(tenant_id)) - timedelta(days=6)).isoformat()

    return int(tenant_id), branch_id, str(dt_ini), str(dt_fim)


master_email = str(os.getenv("PLATFORM_MASTER_EMAIL") or "").strip()
master_password = str(os.getenv("PLATFORM_MASTER_PASSWORD") or "").strip()
if not master_email or not master_password:
    raise RuntimeError("PLATFORM_MASTER_EMAIL/PLATFORM_MASTER_PASSWORD are missing inside the API container.")

report: dict[str, object] = {}

status, internal_health = request_json("GET", API_BASE, "/health")
if status != 200 or not internal_health.get("ok"):
    raise RuntimeError(f"Internal API health failed: status={status} body={internal_health}")
report["api_health"] = internal_health

status, public_health = request_json("GET", PUBLIC_BASE, "/health")
if status != 200 or not public_health.get("ok"):
    raise RuntimeError(f"Public nginx health failed: status={status} body={public_health}")
report["public_health"] = public_health

status, login_body = request_json(
    "POST",
    API_BASE,
    "/auth/login",
    payload={"identifier": master_email, "password": master_password},
)
if status != 200 or not login_body.get("access_token"):
    raise RuntimeError(f"Login smoke failed: status={status} body={login_body}")
report["login"] = {
    "role": login_body.get("role"),
    "user_role": login_body.get("user_role"),
    "home_path": login_body.get("home_path"),
}

token = str(login_body["access_token"])
status, me_body = request_json("GET", API_BASE, "/auth/me", headers={"Authorization": f"Bearer {token}"})
if status != 200:
    raise RuntimeError(f"/auth/me failed: status={status} body={me_body}")
ensure_keys(me_body, "access", "accesses")
report["auth_me"] = {
    "user_role": me_body.get("user_role"),
    "tenant_ids": me_body.get("tenant_ids"),
    "branch_ids": me_body.get("branch_ids"),
    "default_scope": me_body.get("default_scope"),
}

tenant_id, branch_id, dt_ini, dt_fim = build_scope()
query = [("id_empresa", str(tenant_id)), ("dt_ini", dt_ini), ("dt_fim", dt_fim)]
if branch_id is not None:
    query.append(("id_filial", str(branch_id)))
query_string = urllib.parse.urlencode(query)

status, sync_body = request_json(
    "GET",
    API_BASE,
    f"/bi/sync/status?{urllib.parse.urlencode([('id_empresa', str(tenant_id))] + ([('id_filial', str(branch_id))] if branch_id is not None else []))}",
    headers={"Authorization": f"Bearer {token}"},
)
if status != 200:
    raise RuntimeError(f"/bi/sync/status failed: status={status} body={sync_body}")
report["sync_status"] = sync_body

module_checks = [
    ("dashboard_home", f"/bi/dashboard/home?{query_string}", ("overview", "cash", "finance", "churn", "scope")),
    ("sales", f"/bi/sales/overview?{query_string}", ("kpis", "reading_status", "freshness")),
    ("cash", f"/bi/cash/overview?{query_string}", ("historical", "live_now", "kpis", "payment_mix")),
    ("finance", f"/bi/finance/overview?{query_string}", ("aging", "business_clock", "kpis")),
    ("fraud", f"/bi/fraud/overview?{query_string}", ("kpis", "risk_kpis", "business_clock")),
]
module_report: dict[str, object] = {}
for module_name, path, required_keys in module_checks:
    status, body = request_json("GET", API_BASE, path, headers={"Authorization": f"Bearer {token}"})
    if status != 200:
        raise RuntimeError(f"{module_name} smoke failed: status={status} body={body}")
    ensure_keys(body, *required_keys)
    ensure_module_ready(module_name, body)
    module_report[module_name] = {
        "ok": True,
        "keys_checked": list(required_keys),
        "data_state": body.get("data_state"),
        "snapshot_cache": body.get("_snapshot_cache"),
    }
report["modules"] = module_report

with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
    ingest_row = conn.execute(
        """
        SELECT ingest_key
        FROM app.tenants
        WHERE id_empresa = %s
          AND is_active = true
        LIMIT 1
        """,
        (tenant_id,),
    ).fetchone()
if not ingest_row or not ingest_row["ingest_key"]:
    raise RuntimeError(f"Missing ingest_key for tenant {tenant_id}.")

status, ingest_body = request_ingest_dry_run(str(ingest_row["ingest_key"]))
if status != 200 or not ingest_body.get("ok"):
    raise RuntimeError(f"Ingest dry-run failed: status={status} body={ingest_body}")
report["ingest_dry_run"] = {
    "ok": True,
    "tenant_id": tenant_id,
}

if str(os.getenv("RUN_ETL") or "0") == "1":
    status, etl_body = request_json(
        "POST",
        API_BASE,
        f"/etl/run?track=operational&refresh_mart=false&id_empresa={tenant_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    if status == 409 and etl_body.get("error") == "etl_busy":
        report["etl_manual_refresh"] = {
            "status": "busy",
            "message": etl_body.get("detail", {}).get("message"),
        }
    elif status != 200:
        raise RuntimeError(f"ETL smoke failed: status={status} body={etl_body}")
    else:
        report["etl_manual_refresh"] = {
            "status_code": status,
            "body": etl_body,
        }
else:
    report["etl_manual_refresh"] = {
        "status": "skipped",
        "message": "Set RUN_ETL=1 to execute an operational ETL smoke after deploy.",
    }

print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
PY
