"""Scope resolution helpers."""

from __future__ import annotations

from typing import Any, Optional, Sequence, Tuple

from fastapi import HTTPException

from app.authz import claims_access_flag, normalize_role
from app.db import get_conn


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
        if not preferred_tenants:
            raise HTTPException(status_code=403, detail={"error": "tenant_access_missing", "message": "Usuário product_global sem empresas vinculadas."})
        fallback_tenant = preferred_tenants[0]
        id_empresa = int(id_empresa_q or default_tenant or fallback_tenant)
        if int(id_empresa) not in {int(v) for v in preferred_tenants}:
            raise HTTPException(status_code=403, detail={"error": "tenant_access_denied", "message": "Acesso não permitido à empresa."})
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
    active_branch_ids = _active_branch_ids(tenant_id)
    active_branch_set = set(active_branch_ids)

    if user_role in {"platform_master", "platform_admin", "product_global", "channel_admin"}:
        return True, active_branch_ids

    tenant_rows = [row for row in accesses if int(row.get("id_empresa") or 0) == int(tenant_id)]
    if any(row.get("id_filial") is None for row in tenant_rows):
        return True, active_branch_ids

    branch_ids = sorted(
        {
            int(row["id_filial"])
            for row in tenant_rows
            if row.get("id_filial") is not None
            and int(row["id_filial"]) in active_branch_set
        }
    )
    return False, branch_ids


def _active_branch_ids(tenant_id: int) -> list[int]:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        rows = conn.execute(
            """
            SELECT id_filial
            FROM auth.filiais
            WHERE id_empresa = %s
              AND is_active = true
              AND (valid_from IS NULL OR valid_from <= CURRENT_DATE)
              AND (valid_until IS NULL OR valid_until >= CURRENT_DATE)
            ORDER BY id_filial
            """,
            (tenant_id,),
        ).fetchall()
    return [
        int(row["id_filial"])
        for row in rows
        if row.get("id_filial") is not None
    ]


def _materialize_branch_filter(branch_ids: Sequence[int]) -> int | list[int]:
    unique_branch_ids = sorted({int(value) for value in branch_ids if value is not None and int(value) > 0})
    if not unique_branch_ids:
        return []
    if len(unique_branch_ids) == 1:
        return unique_branch_ids[0]
    return unique_branch_ids


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
    allowed_branch_set = set(allowed_branch_ids)

    if requested_branch_ids:
        disallowed = [branch_id for branch_id in requested_branch_ids if branch_id not in allowed_branch_set]
        if disallowed:
            raise HTTPException(
                status_code=403,
                detail={"error": "branch_access_denied", "message": "Acesso não permitido à filial."},
            )
        effective_scope = _materialize_branch_filter(requested_branch_ids)
        return tenant_id, effective_scope, requested_branch_ids

    if default_branch is not None and int(default_branch) in allowed_branch_set:
        return tenant_id, int(default_branch), [int(default_branch)]

    if can_list_all:
        return tenant_id, _materialize_branch_filter(allowed_branch_ids), None

    if allowed_branch_ids:
        branch_id = int(allowed_branch_ids[0])
        return tenant_id, branch_id, [branch_id]

    return tenant_id, [], None


def primary_branch_id(branch_scope: Optional[int | list[int]]) -> Optional[int]:
    if isinstance(branch_scope, list):
        return branch_scope[0] if len(branch_scope) == 1 else None
    return int(branch_scope) if branch_scope is not None else None
