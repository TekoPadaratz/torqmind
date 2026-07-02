"""Branding endpoints.

Two routers:
- ``public_router`` (prefix ``/branding``): serves the image bytes to CSS/`<img>`
  without auth (branding assets are non-sensitive; bearer auth cannot ride a CSS
  ``url()``). Versioned URLs provide cache-busting.
- ``manage_router`` (prefix ``/platform``): permissioned upload/delete, reusing
  the company visibility/mutation guards from ``repos_platform``.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response

from app.deps import get_current_claims
from app import repos_branding, repos_platform

logger = logging.getLogger(__name__)

public_router = APIRouter(prefix="/branding", tags=["branding"])
manage_router = APIRouter(prefix="/platform", tags=["platform", "branding"])

_MAX_UPLOAD = repos_branding.MAX_FILE_BYTES


def _raise(exc: repos_branding.BrandingError) -> None:
    raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())


# --------------------------------------------------------------------------- #
# Public serving (no auth — non-sensitive, needs to load as CSS/img)
# --------------------------------------------------------------------------- #
@public_router.get("/{id_empresa}")
def branding_meta(id_empresa: int):
    """Public branding contract (metadata only) for the active company.

    Lets the frontend apply the right background/logo when a multi-company user
    switches company, without a heavy session reload. Non-sensitive.
    """
    return repos_branding.get_branding_public(id_empresa)


@public_router.get("/{id_empresa}/{kind}")
def serve_branding(id_empresa: int, kind: str):
    try:
        content, mime = repos_branding.serve_image(id_empresa, kind)
    except repos_branding.BrandingError as exc:
        _raise(exc)
    # Long cache: the URL is versioned (?v=hash), so a new upload busts the cache.
    return Response(
        content=content,
        media_type=mime,
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )


# --------------------------------------------------------------------------- #
# Management (permissioned)
# --------------------------------------------------------------------------- #
@manage_router.get("/companies/{tenant_id}/branding")
def branding_detail(tenant_id: int, claims=Depends(get_current_claims)):
    try:
        return repos_branding.get_branding_detail(claims, tenant_id)
    except repos_platform.AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())
    except repos_branding.BrandingError as exc:
        _raise(exc)


async def _read_upload(request: Request) -> bytes:
    """Read the raw request body as the image bytes (no multipart dependency).

    The frontend POSTs the file as the raw body with its image Content-Type.
    Bounded so we never buffer more than the allowed size + 1 byte.
    """
    content = await request.body()
    if len(content) > _MAX_UPLOAD:
        mb = _MAX_UPLOAD // (1024 * 1024)
        raise HTTPException(
            status_code=413,
            detail={"error": "file_too_large", "message": f"Arquivo acima do limite de {mb} MB."},
        )
    return content


@manage_router.post("/companies/{tenant_id}/branding/background")
async def upload_background(
    tenant_id: int,
    request: Request,
    claims=Depends(get_current_claims),
):
    content = await _read_upload(request)
    try:
        return repos_branding.save_image(claims, tenant_id, "background", content)
    except repos_platform.AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())
    except repos_branding.BrandingError as exc:
        _raise(exc)


@manage_router.post("/companies/{tenant_id}/branding/logo")
async def upload_logo(
    tenant_id: int,
    request: Request,
    claims=Depends(get_current_claims),
):
    content = await _read_upload(request)
    try:
        return repos_branding.save_image(claims, tenant_id, "logo", content)
    except repos_platform.AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())
    except repos_branding.BrandingError as exc:
        _raise(exc)


@manage_router.delete("/companies/{tenant_id}/branding/background")
def delete_background(tenant_id: int, claims=Depends(get_current_claims)):
    try:
        return repos_branding.delete_image(claims, tenant_id, "background")
    except repos_platform.AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())
    except repos_branding.BrandingError as exc:
        _raise(exc)


@manage_router.delete("/companies/{tenant_id}/branding/logo")
def delete_logo(tenant_id: int, claims=Depends(get_current_claims)):
    try:
        return repos_branding.delete_image(claims, tenant_id, "logo")
    except repos_platform.AuthError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_detail())
    except repos_branding.BrandingError as exc:
        _raise(exc)
