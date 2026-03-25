"""Scope resolution helpers."""

from __future__ import annotations

from typing import Any, Optional, Sequence, Tuple

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

    if user_role == "channel_admin":
        preferred_tenants = claims.get("tenant_ids") or []
        if not preferred_tenants:
            raise HTTPException(status_code=403, detail={"error": "tenant_access_missing", "message": "Canal sem empresa vinculada ao produto."})
        fallback_tenant = preferred_tenants[0]
        id_empresa = int(id_empresa_q or default_tenant or fallback_tenant)
        if int(id_empresa) not in {int(value) for value in preferred_tenants}:
            raise HTTPException(status_code=403, detail={"error": "tenant_access_denied", "message": "Acesso não permitido à empresa."})
        id_filial = int(id_filial_q or default_branch) if (id_filial_q is not None or default_branch is not None) else None
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

    if user_role in {"platform_master", "platform_admin", "product_global", "channel_admin"}:
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


def _normalize_branch_ids(
    branch_ids: Optional[Sequence[int]] = None,
    branch_id: Optional[int] = None,
) -> list[int]:
    values: list[int] = []
    if branch_ids:
        values.extend(int(value) for value in branch_ids if value is not None)
    if branch_id is not None:
        values.append(int(branch_id))
    return sorted({value for value in values if value > 0})


def resolve_scope_filters(
    claims: dict[str, Any],
    id_empresa_q: Optional[int] = None,
    id_filial_q: Optional[int] = None,
    id_filiais_q: Optional[Sequence[int]] = None,
) -> tuple[int, Optional[int | list[int]], Optional[list[int]]]:
    tenant_id, default_branch = resolve_scope(claims, id_empresa_q=id_empresa_q, id_filial_q=None)
    requested_branch_ids = _normalize_branch_ids(id_filiais_q, id_filial_q)
    can_list_all, allowed_branch_ids = accessible_branch_ids(claims, tenant_id)

    if requested_branch_ids:
        if not can_list_all:
            disallowed = [branch_id for branch_id in requested_branch_ids if branch_id not in allowed_branch_ids]
            if disallowed:
                raise HTTPException(
                    status_code=403,
                    detail={"error": "branch_access_denied", "message": "Acesso não permitido à filial."},
                )
        if len(requested_branch_ids) == 1:
            return tenant_id, requested_branch_ids[0], requested_branch_ids
        return tenant_id, requested_branch_ids, requested_branch_ids

    if default_branch is not None:
        return tenant_id, int(default_branch), [int(default_branch)]

    if can_list_all:
        return tenant_id, None, None

    if allowed_branch_ids:
        branch_id = int(allowed_branch_ids[0])
        return tenant_id, branch_id, [branch_id]

    return tenant_id, None, None


def primary_branch_id(branch_scope: Optional[int | list[int]]) -> Optional[int]:
    if isinstance(branch_scope, list):
        return branch_scope[0] if len(branch_scope) == 1 else None
    return int(branch_scope) if branch_scope is not None else None
