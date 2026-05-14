"""Routes: Competitor Pricing — simplified 5-endpoint API.

Endpoints:
  GET  /bi/pricing/competitor/fuels           — list fuel products + own prices
  POST /bi/pricing/competitor/captures        — register a full capture
  GET  /bi/pricing/competitor/history         — captures for a date
  PATCH /bi/pricing/competitor/items/{item_id} — update a single item price
  GET  /bi/pricing/competitor/comparison      — own vs competitor comparison
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field, validator

from app.deps import get_current_claims
from app import repos_auth
from app.permissions import require_screen
from app.scope import resolve_scope_filters, primary_branch_id
from app import repos_competitor_pricing as pricing_repo

router = APIRouter(prefix="/bi/pricing/competitor", tags=["competitor-pricing"])
logger = logging.getLogger(__name__)

ALLOWED_ROLES = {"MASTER", "OWNER", "MANAGER"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _require_write_access(claims: dict) -> None:
    role = claims["role"]
    if role not in ALLOWED_ROLES:
        raise HTTPException(status_code=403, detail="forbidden")
    try:
        repos_auth.assert_product_write_allowed(claims)
    except repos_auth.AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())


def _extract_scope(claims: dict, id_empresa_q=None, id_filial_q=None):
    tenant, filial_scope, _ = resolve_scope_filters(
        claims, id_empresa_q=id_empresa_q, id_filial_q=id_filial_q,
    )
    filial = primary_branch_id(filial_scope)
    if filial is None:
        raise HTTPException(status_code=400, detail="id_filial é obrigatório.")
    return claims["role"], tenant, filial


def _user_info(claims: dict):
    return str(claims.get("sub") or ""), str(claims.get("name") or claims.get("email") or "")


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------
class CaptureItemIn(BaseModel):
    product_id: int
    price: str = Field(..., description="Preço como string decimal, ex: '5.899'")

    @validator("price")
    def validate_price(cls, v):
        try:
            d = Decimal(v)
        except (InvalidOperation, ValueError):
            raise ValueError("Preço inválido.")
        if d <= 0:
            raise ValueError("Preço deve ser maior que zero.")
        return v


class CaptureCreateRequest(BaseModel):
    station_name: str = Field(..., min_length=3, max_length=200)
    capture_date: date
    observation: Optional[str] = Field(None, max_length=500)
    items: List[CaptureItemIn] = Field(..., min_length=1)


class ItemPriceUpdateRequest(BaseModel):
    new_price: str = Field(..., description="Novo preço como string decimal")
    change_reason: Optional[str] = Field(None, max_length=500)

    @validator("new_price")
    def validate_price(cls, v):
        try:
            d = Decimal(v)
        except (InvalidOperation, ValueError):
            raise ValueError("Preço inválido.")
        if d <= 0:
            raise ValueError("Preço deve ser maior que zero.")
        return v


# ---------------------------------------------------------------------------
# 1) GET /fuels — list fuel products + own price
# ---------------------------------------------------------------------------
@router.get("/fuels")
def get_fuel_products(
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("competitor_pricing")),
):
    role, tenant, filial = _extract_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    try:
        fuels = pricing_repo.list_fuel_products(role, tenant, filial)
        return {"ok": True, "data": fuels}
    except Exception:
        logger.exception("Error listing fuel products")
        raise HTTPException(status_code=500, detail="Erro ao listar combustíveis.")


# ---------------------------------------------------------------------------
# 2) POST /captures — register a full capture
# ---------------------------------------------------------------------------
@router.post("/captures")
def create_capture(
    body: CaptureCreateRequest,
    request: Request,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("competitor_pricing")),
):
    _require_write_access(claims)
    role, tenant, filial = _extract_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    user_id, user_name = _user_info(claims)

    items_dicts = [{"product_id": it.product_id, "price": it.price} for it in body.items]

    try:
        result = pricing_repo.create_capture(
            role=role,
            id_empresa=tenant,
            id_filial=filial,
            station_name=body.station_name,
            capture_date=body.capture_date,
            observation=body.observation,
            items=items_dicts,
            user_id=user_id,
            user_name=user_name,
            client_ip=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
        )
        return {"ok": True, "data": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Error creating capture")
        raise HTTPException(status_code=500, detail="Erro ao registrar captura.")


# ---------------------------------------------------------------------------
# 3) GET /history — captures for a date
# ---------------------------------------------------------------------------
@router.get("/history")
def get_history(
    capture_date: date = Query(..., description="Data no formato YYYY-MM-DD"),
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("competitor_pricing")),
):
    role, tenant, filial = _extract_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    try:
        history = pricing_repo.list_history(role, tenant, filial, capture_date)
        return {"ok": True, "data": history}
    except Exception:
        logger.exception("Error listing history")
        raise HTTPException(status_code=500, detail="Erro ao buscar histórico.")


# ---------------------------------------------------------------------------
# 4) PATCH /items/{item_id} — update a single item price
# ---------------------------------------------------------------------------
@router.patch("/items/{item_id}")
def update_item_price(
    item_id: str,
    body: ItemPriceUpdateRequest,
    request: Request,
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("competitor_pricing")),
):
    _require_write_access(claims)
    role, tenant, filial = _extract_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    user_id, user_name = _user_info(claims)

    try:
        result = pricing_repo.update_item_price(
            role=role,
            id_empresa=tenant,
            id_filial=filial,
            item_id=item_id,
            new_price=Decimal(body.new_price),
            change_reason=body.change_reason,
            user_id=user_id,
            user_name=user_name,
            client_ip=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
        )
        return {"ok": True, "data": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Error updating item price")
        raise HTTPException(status_code=500, detail="Erro ao atualizar preço.")


# ---------------------------------------------------------------------------
# 5) GET /comparison — own vs competitor comparison
# ---------------------------------------------------------------------------
@router.get("/comparison")
def get_comparison(
    capture_date: date = Query(..., description="Data no formato YYYY-MM-DD"),
    id_filial: Optional[int] = Query(None),
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
    _screen=Depends(require_screen("competitor_pricing")),
):
    role, tenant, filial = _extract_scope(claims, id_empresa_q=id_empresa, id_filial_q=id_filial)
    try:
        comparison = pricing_repo.get_comparison(role, tenant, filial, capture_date)
        return {"ok": True, "data": comparison}
    except Exception:
        logger.exception("Error building comparison")
        raise HTTPException(status_code=500, detail="Erro ao gerar comparativo.")
