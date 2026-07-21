#!/usr/bin/env python3
"""Smoke HTTP: clientes preço fixo em homolog :81."""
from __future__ import annotations

import json
import urllib.request
from datetime import date, timedelta

from app.security import create_access_token

BASE = "http://127.0.0.1:81/api"
token = create_access_token(
    {
        "sub": "1",
        "role": "platform_master",
        "id_empresa": 1,
        "id_usuario": 1,
        "email": "master@torqmind.local",
    }
)
dt_fim = date.today()
dt_ini = dt_fim - timedelta(days=14)


def get(path: str) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"{BASE}{path}",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = json.loads(resp.read().decode())
        return resp.status, body


status, ov = get(
    f"/bi/customers/preco-fixo?dt_ini={dt_ini}&dt_fim={dt_fim}&id_empresa=1&page=0&page_size=5"
)
print("overview", status, ov.get("source"), ov.get("total"), ov.get("summary"))
for row in (ov.get("items") or [])[:3]:
    print(" ", row)

status, det = get(
    f"/bi/customers/preco-fixo/detail?dt_ini={dt_ini}&dt_fim={dt_fim}"
    f"&id_empresa=1&id_filial=10169&id_entidade=7527&page=0&page_size=3"
)
print("detail7527", status, det.get("cliente_nome"), det.get("summary"))
for row in (det.get("items") or [])[:3]:
    print(" ", row)
