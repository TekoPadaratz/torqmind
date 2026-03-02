from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException

from app.db import get_conn
from app.deps import get_current_claims
from app.scope import resolve_scope

router = APIRouter(prefix="/etl", tags=["etl"])


@router.post("/run")
def run_etl(
    refresh_mart: bool = Query(True),
    force_full: bool = Query(False),
    id_empresa: Optional[int] = Query(None, description="Only used by MASTER"),
    claims=Depends(get_current_claims),
):
    """Run the STG→DW→MART pipeline for a tenant.

    PT-BR: Esse endpoint é o botão "atualizar dados" do seu BI.
    EN   : This is the "refresh" button for your BI.

    Security:
    - MASTER can run for any tenant (id_empresa query param).
    - OWNER/MANAGER can run only for their tenant.
    """

    role = claims["role"]
    tenant, _ = resolve_scope(claims, id_empresa_q=id_empresa, id_filial_q=None)

    # Managers typically should not run ETL in production, but for dev we allow.
    with get_conn(role=role, tenant_id=tenant, branch_id=None) as conn:
        try:
            row = conn.execute(
                "SELECT etl.run_all(%s, %s, %s) AS result",
                (tenant, force_full, refresh_mart),
            ).fetchone()
            conn.commit()
            return row["result"]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"ETL failed: {e}")
