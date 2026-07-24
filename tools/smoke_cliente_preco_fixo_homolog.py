#!/usr/bin/env python3
"""Smoke HTTP: clientes preço fixo em homolog :81."""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from datetime import date, timedelta

from app.db import get_conn
from app.security import create_access_token

BASE = os.environ.get("SMOKE_BASE_URL", "http://127.0.0.1:81/api")


def _master_user_id() -> str:
    env_uid = (os.environ.get("SMOKE_USER_ID") or "").strip()
    if env_uid:
        return env_uid
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(
            """
            SELECT u.id::text AS id
            FROM auth.users u
            INNER JOIN auth.user_tenants ut ON ut.user_id = u.id
            WHERE ut.role IN ('platform_master', 'MASTER', 'master')
            ORDER BY u.created_at NULLS LAST
            LIMIT 1
            """
        ).fetchone()
    if not row or not row.get("id"):
        raise SystemExit("Nenhum platform_master encontrado para smoke auth")
    return str(row["id"])


token = create_access_token(
    {
        "sub": _master_user_id(),
        "role": "MASTER",
        "user_role": "platform_master",
        "id_empresa": 1,
        "email": "smoke@torqmind.local",
        "can_view_sensitive_financials": True,
    }
)
dt_fim = date.today()
dt_ini = dt_fim - timedelta(days=30)


def get(path: str) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"{BASE}{path}",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            body = json.loads(resp.read().decode())
            return resp.status, body
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:500]
        raise SystemExit(f"HTTP {exc.code} on {path}: {detail}") from exc


status, ov = get(
    f"/bi/customers/preco-fixo?dt_ini={dt_ini}&dt_fim={dt_fim}&id_empresa=1&page=0&page_size=5"
)
summary = ov.get("summary") or {}
print(
    "overview",
    status,
    ov.get("source"),
    "total=",
    ov.get("total"),
    "litros=",
    summary.get("qtd_litros"),
    "desconto=",
    summary.get("desconto_total"),
)
assert ov.get("source") == "clickhouse_mart", ov
assert int(ov.get("total") or 0) > 0, "grid vazio"
assert float(summary.get("qtd_litros") or 0) > 0, "summary sem litros"
for row in (ov.get("items") or [])[:3]:
    print(
        " ",
        row.get("filial_label"),
        row.get("cliente_nome"),
        "L=",
        row.get("qtd_litros"),
        "R$",
        row.get("desconto_total"),
    )
    assert "qtd_litros" in row

first = (ov.get("items") or [None])[0]
assert first, "sem itens no overview"
status, det = get(
    f"/bi/customers/preco-fixo/detail?dt_ini={dt_ini}&dt_fim={dt_fim}"
    f"&id_empresa=1&id_filial={first['id_filial']}&id_entidade={first['id_entidade']}"
    f"&page=0&page_size=50"
)
items = det.get("items") or []
print(
    "detail",
    status,
    det.get("cliente_nome"),
    "summary=",
    det.get("summary"),
    "rows=",
    len(items),
)
assert any(r.get("row_kind") == "item" for r in items), "sem itens no detail"
assert any(r.get("row_kind") == "subtotal" for r in items), "sem subtotal por produto"
# Ordem Data → Documento no payload do item
item0 = next(r for r in items if r.get("row_kind") == "item")
assert "dt_venda" in item0 and "documento_label" in item0
for row in items[:4]:
    print(
        " ",
        row.get("row_kind"),
        row.get("dt_venda"),
        row.get("documento_label"),
        row.get("produto_nome"),
        row.get("margem_unitaria_pct"),
    )

status, tg = get("/bi/me/telegram?id_empresa=1")
catalog = [x.get("key") for x in (tg.get("alert_catalog") or [])]
print("telegram", status, "catalog=", catalog)
assert "PRECO_FIXO_BOMBA_DESATUALIZADO" in catalog
assert "VENDA_CANCELADA" in catalog
print("SMOKE_PASS")
