"""Scope resolution helpers.

PT-BR: Resolve qual id_empresa / id_filial efetivo deve ser usado com base no role + query params.
EN   : Resolve effective id_empresa / id_filial based on role + query params.

Rules:
- MASTER: can query any tenant/branch (defaults to id_empresa=1).
- OWNER : fixed tenant, can optionally filter by branch.
- MANAGER: fixed tenant+branch, cannot change.
"""

from __future__ import annotations

from typing import Any, Optional, Tuple


def resolve_scope(
    claims: dict[str, Any],
    id_empresa_q: Optional[int] = None,
    id_filial_q: Optional[int] = None,
) -> Tuple[int, Optional[int]]:
    role = claims.get("role")

    if role == "MASTER":
        id_empresa = int(id_empresa_q or 1)
        id_filial = int(id_filial_q) if id_filial_q is not None else None
        return id_empresa, id_filial

    # Owner/Manager must have tenant.
    id_empresa_claim = claims.get("id_empresa")
    if id_empresa_claim is None:
        raise ValueError("Missing tenant in token")
    id_empresa = int(id_empresa_claim)

    if role == "OWNER":
        id_filial = int(id_filial_q) if id_filial_q is not None else None
        return id_empresa, id_filial

    if role == "MANAGER":
        id_filial_claim = claims.get("id_filial")
        if id_filial_claim is None:
            raise ValueError("Missing branch in token")
        return id_empresa, int(id_filial_claim)

    raise ValueError(f"Unknown role: {role}")
