from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from app.config import settings
from app.db import get_conn

from app.routes_auth import router as auth_router
from app.routes_dashboard import router as dashboard_router
from app.routes_ingest import router as ingest_router
from app.routes_etl import router as etl_router
from app.routes_bi import router as bi_router
from app.routes_platform import router as platform_router

logger = logging.getLogger(__name__)


def _ensure_dev_seed() -> None:
    """Auto-bootstrap seed in dev/local when auth.users is empty.

    This avoids repeated login failures after container/database recreation.
    No-op outside dev/local.
    """

    if settings.app_env.lower() not in {"dev", "local"}:
        return

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM auth.users").fetchone() or {"total": 0}
        total = int(row.get("total", 0) or 0)

    if total > 0:
        return

    from app.cli.seed import main as seed_main

    seed_main()

app = FastAPI(title="TorqMind API", version="0.2.1", root_path=settings.app_root_path or "")

cors_origins = [item.strip() for item in str(settings.app_cors_origins or "").split(",") if item.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_origin_regex=settings.app_cors_origin_regex or None,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(bi_router)
app.include_router(platform_router)
app.include_router(etl_router)
app.include_router(ingest_router)


@app.on_event("startup")
def startup_event() -> None:
    _ensure_dev_seed()


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException):
    detail = exc.detail
    if isinstance(detail, dict):
        error = str(detail.get("error") or "http_error")
        return JSONResponse(status_code=exc.status_code, content={"error": error, "detail": detail})
    return JSONResponse(status_code=exc.status_code, content={"error": str(detail or "http_error")})


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_: Request, exc: RequestValidationError):
    return JSONResponse(status_code=422, content={"error": "validation_error", "detail": exc.errors()})


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path, exc_info=exc)
    return JSONResponse(
        status_code=500,
        content={
            "error": "internal_error",
            "detail": {"message": "Falha interna do servidor. Tente novamente em instantes."},
        },
    )


@app.get("/")
def root():
    # PT-BR: Ajuda a evitar confusão ao abrir localhost:8000 no browser.
    # EN: Prevents confusion when opening localhost:8000 in the browser.
    return {"ok": True, "service": "torqmind-api", "docs": "/docs", "health": "/health"}


@app.get("/health")
def health():
    try:
        with get_conn() as conn:
            row = conn.execute("SELECT current_database() AS db, now() AS now").fetchone()
        return {"ok": True, "status": "up", "db": row["db"], "time": str(row["now"])}
    except Exception as exc:
        return {"ok": False, "status": "degraded", "error": str(exc)}


@app.get("/debug/db")
def debug_db():
    if settings.app_env.lower() not in {"dev", "local"}:
        raise HTTPException(status_code=404, detail="Not found")

    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT
              current_database() AS current_database,
              inet_server_addr()::text AS inet_server_addr,
              inet_server_port() AS inet_server_port
            """
        ).fetchone()
    return dict(row)
