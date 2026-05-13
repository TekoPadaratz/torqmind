"""Routes for competitor pricing management.

CRUD for competitor stations, price captures, and fuel product listing.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.deps import get_current_claims
from app import repos_auth
from app.scope import resolve_scope_filters, primary_branch_id
from app import repos_competitor_pricing as pricing_repo

router = APIRouter(prefix="/bi/pricing/competitor", tags=["competitor-pricing"])
logger = logging.getLogger(__name__)

ALLOWED_ROLES = {"MASTER", "OWNER", "MANAGER"}


def _require_write_access(claims: dict) -> None:
    role = claims["role"]
    if role not in ALLOWED_ROLES:
        raise HTTPException(status_code=403, detail="forbidden")
    try:
        repos_auth.assert_product_write_allowed(claims)
    except repos_auth.AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())


def _extract_scope(claims: dict, id_empresa_q=None, id_filial_q=None, id_filiais_q=None):
    tenant, filial_scope, branch_scope = resolve_scope_filters(
        claims, id_empresa_q=id_empresa_q, id_filial_q=id_filial_q, id_filiais_q=id_filiais_q,
    )
    filial = primary_branch_id(filial_scope)
    if filial is None:
        raise HTTPException(status_code=400, detail="id_filial is required for competitor pricing")
    return claims["role"], tenant, filial


def _user_info(claims: dict):
    return str(claims.get("sub") or ""), str(claims.get("name") or claims.get("email") or "")


# ---------------------------------------------------------------------------
# Stations
# ---------------------------------------------------------------------------

class StationCreateRequest(BaseModel):
    station_name: str = Field(..., min_length=2, max_length=200)
    document_number: Optional[str] = None
    address_text: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class StationUpdateRequest(BaseModel):
    station_name: Optional[str] = Field(None, min_length=2, max_length=200)
    document_number: Optional[str] = None
    address_text: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    is_active: Optional[bool] = None


@router.get("/stations")
def list_stations(
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
):
    role, tenant, filial = _extract_scope(claims, id_empresa, id_filial, id_filiais)
    items = pricing_repo.list_stations(role, tenant, filial)
    return {"items": items}


@router.post("/stations")
def create_station(
    body: StationCreateRequest,
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
):
    _require_write_access(claims)
    role, tenant, filial = _extract_scope(claims, id_empresa, id_filial, id_filiais)
    user_id, user_name = _user_info(claims)
    try:
        result = pricing_repo.create_station(
            role, tenant, filial,
            station_name=body.station_name,
            document_number=body.document_number,
            address_text=body.address_text,
            city=body.city,
            state=body.state,
            latitude=body.latitude,
            longitude=body.longitude,
            user_id=user_id,
            user_name=user_name,
        )
        return {"ok": True, **result}
    except Exception as exc:
        if "uq_competitor_stations_name_active" in str(exc):
            raise HTTPException(status_code=409, detail="Posto com este nome já existe para esta filial.")
        raise


@router.put("/stations/{station_id}")
def update_station(
    station_id: str,
    body: StationUpdateRequest,
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
):
    _require_write_access(claims)
    role, tenant, filial = _extract_scope(claims, id_empresa, id_filial, id_filiais)
    user_id, user_name = _user_info(claims)
    result = pricing_repo.update_station(
        role, tenant, filial, station_id,
        station_name=body.station_name,
        document_number=body.document_number,
        address_text=body.address_text,
        city=body.city,
        state=body.state,
        latitude=body.latitude,
        longitude=body.longitude,
        is_active=body.is_active,
        user_id=user_id,
        user_name=user_name,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Posto não encontrado.")
    return {"ok": True, **result}


@router.delete("/stations/{station_id}")
def delete_station(
    station_id: str,
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
):
    _require_write_access(claims)
    role, tenant, filial = _extract_scope(claims, id_empresa, id_filial, id_filiais)
    user_id, user_name = _user_info(claims)
    ok = pricing_repo.soft_delete_station(
        role, tenant, filial, station_id,
        user_id=user_id,
        user_name=user_name,
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Posto não encontrado.")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Fuel Products (read-only, for capture form)
# ---------------------------------------------------------------------------

@router.get("/fuel-products")
def list_fuel_products(
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
):
    role, tenant, filial = _extract_scope(claims, id_empresa, id_filial, id_filiais)
    items = pricing_repo.list_fuel_products(role, tenant, filial)
    return {"items": items}


# ---------------------------------------------------------------------------
# Captures
# ---------------------------------------------------------------------------

class CaptureItemRequest(BaseModel):
    id_produto: int
    price: float = Field(..., gt=0)
    product_name: Optional[str] = ""
    fuel_type: Optional[str] = ""


class CaptureUpsertRequest(BaseModel):
    station_id: str
    capture_date: date
    items: List[CaptureItemRequest] = Field(..., min_length=1)
    observation: Optional[str] = None
    geo_latitude: Optional[float] = None
    geo_longitude: Optional[float] = None
    geo_accuracy_meters: Optional[float] = None


@router.post("/captures")
def upsert_capture(
    body: CaptureUpsertRequest,
    request: Request,
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
):
    _require_write_access(claims)
    role, tenant, filial = _extract_scope(claims, id_empresa, id_filial, id_filiais)
    user_id, user_name = _user_info(claims)

    client_ip = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent", "")[:500]

    items = [
        {"id_produto": it.id_produto, "price": it.price, "product_name": it.product_name or "", "fuel_type": it.fuel_type or ""}
        for it in body.items
    ]

    try:
        result = pricing_repo.upsert_capture(
            role, tenant, filial,
            station_id=body.station_id,
            capture_date=body.capture_date,
            items=items,
            observation=body.observation,
            source="WEB",
            client_ip=client_ip,
            user_agent=user_agent,
            geo_latitude=body.geo_latitude,
            geo_longitude=body.geo_longitude,
            geo_accuracy_meters=body.geo_accuracy_meters,
            user_id=user_id,
            user_name=user_name,
        )
        return {"ok": True, **result}
    except ValueError as exc:
        if "station_not_found" in str(exc):
            raise HTTPException(status_code=404, detail="Posto concorrente não encontrado.")
        raise


@router.get("/captures")
def list_captures(
    dt_ini: Optional[date] = Query(None),
    dt_fim: Optional[date] = Query(None),
    station_id: Optional[str] = Query(None),
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
):
    role, tenant, filial = _extract_scope(claims, id_empresa, id_filial, id_filiais)
    items = pricing_repo.list_captures(
        role, tenant, filial,
        station_id=station_id,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
    )
    return {"items": items}


@router.get("/captures/{capture_id}")
def get_capture_detail(
    capture_id: str,
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
):
    role, tenant, filial = _extract_scope(claims, id_empresa, id_filial, id_filiais)
    result = pricing_repo.get_capture_detail(role, tenant, filial, capture_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Captura não encontrada.")
    return result


@router.delete("/captures/{capture_id}")
def delete_capture(
    capture_id: str,
    id_filial: Optional[int] = Query(None),
    id_filiais: Optional[List[int]] = Query(None),
    id_empresa: Optional[int] = Query(None),
    claims=Depends(get_current_claims),
):
    _require_write_access(claims)
    role, tenant, filial = _extract_scope(claims, id_empresa, id_filial, id_filiais)
    user_id, user_name = _user_info(claims)
    ok = pricing_repo.delete_capture(
        role, tenant, filial, capture_id,
        user_id=user_id,
        user_name=user_name,
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Captura não encontrada.")
    return {"ok": True}
