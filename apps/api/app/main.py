from __future__ import annotations

import logging

# Abort boot before wiring routes if prod/homolog stacks are crossed.
from app.runtime_guard import assert_runtime_stack_or_exit

assert_runtime_stack_or_exit()

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.middleware.base import BaseHTTPMiddleware

from app.config import settings
from app.db import get_conn
from app.security import decode_token

from app.routes_auth import router as auth_router
from app.routes_mfa import router as mfa_router
from app.routes_dashboard import router as dashboard_router
from app.routes_ingest import router as ingest_router
from app.routes_etl import router as etl_router
from app.routes_bi import router as bi_router
from app.routes_competitor_pricing import router as competitor_pricing_router
from app.routes_platform import router as platform_router
from app.routes_tv import router as tv_router
from app.routes_profit import router as profit_router
from app.routes_branding import public_router as branding_public_router, manage_router as branding_manage_router
from app.routes_agent_update import router as agent_update_router
from app.routes_ai import router as ai_router

logger = logging.getLogger(__name__)
_startup_status: dict[str, str | bool | None] = {"ok": True, "message": None}


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

_is_prod = settings.app_env.lower() in {"prod", "production"}

app = FastAPI(
    title="TorqMind API",
    version="0.2.1",
    root_path=settings.app_root_path or "",
    docs_url=None if _is_prod else "/docs",
    redoc_url=None if _is_prod else "/redoc",
    openapi_url=None if _is_prod else "/openapi.json",
)

cors_origins = [item.strip() for item in str(settings.app_cors_origins or "").split(",") if item.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_origin_regex=settings.app_cors_origin_regex or None,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


# ── Force password change middleware ─────────────────────────
_PASSWORD_CHANGE_EXEMPT = {
    "/auth/login",
    "/auth/logout",
    "/auth/change-password",
    "/auth/me",
    "/auth/mfa/status",
    "/auth/mfa/verify",
    "/auth/mfa/setup/start",
    "/auth/mfa/setup/confirm",
    "/health",
    "/readyz",
}


class ForcePasswordChangeMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path.rstrip("/")
        if request.method == "OPTIONS" or path in _PASSWORD_CHANGE_EXEMPT:
            return await call_next(request)

        token = None
        auth_header = request.headers.get("authorization", "")
        if auth_header.lower().startswith("bearer "):
            token = auth_header.split(" ", 1)[1].strip()
        else:
            try:
                from app.session_cookies import access_cookie_name

                token = (request.cookies.get(access_cookie_name()) or "").strip() or None
            except Exception:
                token = None
        if token:
            try:
                payload = decode_token(token)
                if payload.get("must_change_password"):
                    return JSONResponse(
                        status_code=403,
                        content={
                            "error": "password_change_required",
                            "message": "You must change your password before accessing this resource.",
                        },
                    )
            except Exception:
                pass  # let downstream auth handle invalid tokens

        return await call_next(request)


app.add_middleware(ForcePasswordChangeMiddleware)


# ── CSRF + Origin for cookie sessions (Prompt 7) ─────────────
_CSRF_ORIGIN_EXEMPT_PREFIXES = (
    "/ingest",
    "/health",
    "/readyz",
)
_CSRF_ORIGIN_EXEMPT_EXACT = {
    "/auth/login",
    "/auth/forgot-password",
    "/auth/reset-password",
    "/auth/logout",
}


class CsrfOriginMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method.upper() in {"GET", "HEAD", "OPTIONS"}:
            return await call_next(request)
        if str(settings.app_env or "").strip().lower() == "test":
            return await call_next(request)
        path = request.url.path.rstrip("/") or "/"
        if path in _CSRF_ORIGIN_EXEMPT_EXACT or any(path.startswith(p) for p in _CSRF_ORIGIN_EXEMPT_PREFIXES):
            return await call_next(request)
        if not bool(getattr(settings, "auth_csrf_enforce", True)):
            return await call_next(request)
        try:
            from app.session_cookies import validate_browser_mutation_origin, validate_csrf

            validate_browser_mutation_origin(request)
            validate_csrf(request)
        except HTTPException as exc:
            detail = exc.detail
            if isinstance(detail, dict):
                return JSONResponse(status_code=exc.status_code, content=detail)
            return JSONResponse(status_code=exc.status_code, content={"error": "forbidden", "message": str(detail)})
        return await call_next(request)


app.add_middleware(CsrfOriginMiddleware)


# ── Security headers middleware ────────────────────────────────
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        if settings.app_env.lower() in {"prod", "production"}:
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        return response


app.add_middleware(SecurityHeadersMiddleware)


# ── Auth rate limiting (identity + optional IP via shared buckets) ─
# IP identity: see app.client_ip — default hops=0 does not trust X-Forwarded-For.
# Cross-worker state: auth.security_attempt_buckets (migration 155) via security_attempts.
_AUTH_MAIL_PATHS = {
    "/auth/forgot-password",
    "/auth/reset-password",
}


class LoginRateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path.rstrip("/")
        if str(settings.app_env or "").strip().lower() == "test":
            return await call_next(request)

        from app.client_ip import client_ip_for_rate_limit
        from app import security_attempts

        if request.method == "POST" and path == "/auth/login":
            ip_key = security_attempts.bucket_key(
                "auth", "login", "ip", client_ip_for_rate_limit(request)
            )
            max_n = int(settings.auth_login_ip_max_per_window)
            window = int(settings.auth_login_ip_window_seconds)
            if security_attempts.is_rate_limited(ip_key, max_attempts=max_n, window_seconds=window):
                return JSONResponse(
                    status_code=429,
                    content={"error": "rate_limited", "message": "Muitas tentativas de login. Aguarde 1 minuto."},
                )
            security_attempts.record_attempt(ip_key, window_seconds=window)
        elif request.method == "POST" and path in _AUTH_MAIL_PATHS:
            ip_key = security_attempts.bucket_key(
                "auth", "mail", "ip", client_ip_for_rate_limit(request)
            )
            max_n = int(settings.auth_mail_ip_max_per_window)
            window = int(settings.auth_mail_ip_window_seconds)
            if security_attempts.is_rate_limited(ip_key, max_attempts=max_n, window_seconds=window):
                return JSONResponse(
                    status_code=429,
                    content={
                        "error": "rate_limited",
                        "message": "Muitas tentativas de recuperação de senha. Aguarde 1 minuto.",
                    },
                )
            security_attempts.record_attempt(ip_key, window_seconds=window)
        return await call_next(request)


app.add_middleware(LoginRateLimitMiddleware)

app.include_router(auth_router)
app.include_router(mfa_router)
app.include_router(dashboard_router)
app.include_router(bi_router)
app.include_router(competitor_pricing_router)
app.include_router(platform_router)
app.include_router(tv_router)
app.include_router(etl_router)
app.include_router(ingest_router)
app.include_router(agent_update_router)
app.include_router(profit_router)
app.include_router(ai_router)
app.include_router(branding_public_router)
app.include_router(branding_manage_router)


@app.on_event("startup")
def startup_event() -> None:
    global _startup_status
    try:
        _ensure_dev_seed()
        _startup_status = {"ok": True, "message": None}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Startup soft-failed while preparing local seed: %s", exc, exc_info=exc)
        # Seed failure in dev/local is non-critical — keep the app healthy.
        _startup_status = {"ok": True, "message": f"seed_skipped: {exc}"}


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
            row = conn.execute("SELECT 1 AS ok, now() AS now").fetchone()
        if _startup_status.get("ok", True):
            return {"ok": True, "status": "up", "time": str(row["now"])}
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "status": "degraded",
                "time": str(row["now"]),
                "startup": {"message": _startup_status.get("message")},
            },
        )
    except Exception:
        return JSONResponse(status_code=503, content={"ok": False, "status": "degraded"})


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
