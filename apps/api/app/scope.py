"""Scope resolution helpers."""

from __future__ import annotations

from typing import Any, Optional, Tuple

from fastapi import HTTPException

from app.authz import claims_access_flag, normalize_role


def resolve_scope(
    claims: dict[str, Any],
    id_empresa_q: Optional[int] = None,
    id_filial_q: Optional[int] = None,
) -> Tuple[int, Optional[int]]:
    if not claims_access_flag(claims, "product"):
        raise HTTPException(status_code=403, detail={"error": "product_forbidden", "message": "Acesso ao produto não permitido."})

    user_role = normalize_role(claims.get("user_role"))
    accesses = [row for row in (claims.get("accesses") or []) if row.get("id_empresa") is not None]
    default_tenant = claims.get("id_empresa")
    default_branch = claims.get("id_filial")

    if user_role in {"platform_master", "platform_admin"}:
        id_empresa = int(id_empresa_q or default_tenant or 1)
        id_filial = int(id_filial_q) if id_filial_q is not None else None
        return id_empresa, id_filial

    if user_role == "product_global":
        preferred_tenants = claims.get("tenant_ids") or []
        fallback_tenant = preferred_tenants[0] if preferred_tenants else 1
        id_empresa = int(id_empresa_q or default_tenant or fallback_tenant)
        id_filial = int(id_filial_q or default_branch) if (id_filial_q is not None or default_branch is not None) else None
        return id_empresa, id_filial

    if not accesses:
        raise HTTPException(status_code=403, detail={"error": "tenant_access_missing", "message": "Usuário sem empresa vinculada."})

    tenant_id = int(id_empresa_q or default_tenant or accesses[0]["id_empresa"])
    tenant_rows = [row for row in accesses if int(row["id_empresa"]) == tenant_id]
    if not tenant_rows:
        raise HTTPException(status_code=403, detail={"error": "tenant_access_denied", "message": "Acesso não permitido à empresa."})

    all_branches_allowed = any(row.get("id_filial") is None for row in tenant_rows)
    requested_branch = id_filial_q
    if requested_branch is None and default_branch is not None:
        requested_branch = int(default_branch)

    if requested_branch is None:
        if all_branches_allowed:
            return tenant_id, None
        first_branch = next((row.get("id_filial") for row in tenant_rows if row.get("id_filial") is not None), None)
        return tenant_id, int(first_branch) if first_branch is not None else None

    if all_branches_allowed:
        return tenant_id, int(requested_branch)

    if any(int(row.get("id_filial") or 0) == int(requested_branch) for row in tenant_rows):
        return tenant_id, int(requested_branch)

    raise HTTPException(status_code=403, detail={"error": "branch_access_denied", "message": "Acesso não permitido à filial."})


def accessible_branch_ids(claims: dict[str, Any], tenant_id: int) -> tuple[bool, list[int]]:
    user_role = normalize_role(claims.get("user_role"))
    accesses = [row for row in (claims.get("accesses") or []) if row.get("id_empresa") is not None]

    if user_role in {"platform_master", "platform_admin", "product_global"}:
        return True, []

    tenant_rows = [row for row in accesses if int(row.get("id_empresa") or 0) == int(tenant_id)]
    if any(row.get("id_filial") is None for row in tenant_rows):
        return True, []

    branch_ids = sorted(
        {
            int(row["id_filial"])
            for row in tenant_rows
            if row.get("id_filial") is not None
        }
    )
    return False, branch_ids
