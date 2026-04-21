from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from app.business_time import business_today, business_timezone_name
from app.authz import (
    analytics_role_for_user_role,
    can_access_platform,
    can_access_product,
    can_manage_platform_finance,
    can_manage_platform_operations,
    is_sovereign_email,
    is_date_in_window,
    is_product_readonly_role,
    normalize_role,
    role_label,
    role_priority,
    tenant_status_allows_login,
    tenant_status_warning_message,
    tenant_status_is_warning,
)
from app.db import get_conn
from app.security import verify_password
from app.usernames import (
    identifier_looks_like_email,
    is_valid_username,
    normalize_email,
    normalize_login_identifier,
    normalize_username,
)

LOCK_AFTER_FAILURES = 5
LOCK_WINDOW_MINUTES = 15
PRODUCT_SCOPE_CACHE_TTL_SECONDS = 60.0
DUMMY_PASSWORD_HASH = "$2b$12$TpwRPxMEpx/YoXq784S/Ue136/CMtKT9iNZivMWqAcEPOeUAOX7oW"

_product_scope_cache: dict[tuple[int, int | None], tuple[float, dict[str, Any]]] = {}


class AuthError(Exception):
    def __init__(self, status_code: int, error: str, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.error = error
        self.message = message

    def as_detail(self) -> dict[str, str]:
        return {"error": self.error, "message": self.message}


def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    normalized_email = normalize_email(email)
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        return conn.execute(
            """
            SELECT
              id,
              email,
              username,
              password_hash,
              is_active,
              nome,
              role,
              valid_from,
              valid_until,
              must_change_password,
              last_login_at,
              failed_login_count,
              locked_until,
              created_at,
              updated_at
            FROM auth.users
            WHERE lower(email) = %s
            """,
            (normalized_email,),
        ).fetchone()


def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    normalized_username = normalize_username(username)
    if not is_valid_username(normalized_username):
        return None

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        return conn.execute(
            """
            SELECT
              id,
              email,
              username,
              password_hash,
              is_active,
              nome,
              role,
              valid_from,
              valid_until,
              must_change_password,
              last_login_at,
              failed_login_count,
              locked_until,
              created_at,
              updated_at
            FROM auth.users
            WHERE username = %s
            """,
            (normalized_username,),
        ).fetchone()


def get_user_by_identifier(identifier: str) -> Optional[Dict[str, Any]]:
    normalized_identifier = normalize_login_identifier(identifier)
    if not normalized_identifier:
        return None
    if identifier_looks_like_email(normalized_identifier):
        return get_user_by_email(normalized_identifier)
    return get_user_by_username(normalized_identifier)


def get_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        return conn.execute(
            """
            SELECT
              id,
              email,
              username,
              password_hash,
              is_active,
              nome,
              role,
              valid_from,
              valid_until,
              must_change_password,
              last_login_at,
              failed_login_count,
              locked_until,
              created_at,
              updated_at
            FROM auth.users
            WHERE id = %s::uuid
            """,
            (user_id,),
        ).fetchone()


def _list_user_access_rows(user_id: str) -> list[dict[str, Any]]:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        rows = conn.execute(
            """
            SELECT
              ut.user_id,
              ut.role,
              ut.channel_id,
              ut.id_empresa,
              ut.id_filial,
              ut.is_enabled,
              ut.valid_from,
              ut.valid_until,
              ut.created_at,
              ut.updated_at,
              c.name AS channel_name,
              c.is_enabled AS channel_is_enabled,
              t.nome AS tenant_name,
              t.is_active AS tenant_is_enabled,
              t.status AS tenant_status,
              t.valid_from AS tenant_valid_from,
              t.valid_until AS tenant_valid_until,
              t.billing_status AS tenant_billing_status,
              t.grace_until AS tenant_grace_until,
              t.channel_id AS tenant_channel_id,
              f.nome AS branch_name,
              f.is_active AS branch_is_enabled,
              f.valid_from AS branch_valid_from,
              f.valid_until AS branch_valid_until,
              f.blocked_reason AS branch_blocked_reason
            FROM auth.user_tenants ut
            LEFT JOIN app.channels c
              ON c.id = ut.channel_id
            LEFT JOIN app.tenants t
              ON t.id_empresa = ut.id_empresa
            LEFT JOIN auth.filiais f
              ON f.id_empresa = ut.id_empresa
             AND f.id_filial = ut.id_filial
            WHERE ut.user_id = %s::uuid
            ORDER BY
              CASE ut.role
                WHEN 'platform_master' THEN 0
                WHEN 'platform_admin' THEN 1
                WHEN 'channel_admin' THEN 2
                WHEN 'tenant_admin' THEN 3
                WHEN 'tenant_manager' THEN 4
                WHEN 'tenant_viewer' THEN 5
                ELSE 99
              END,
              ut.id_empresa NULLS FIRST,
              ut.id_filial NULLS FIRST,
              ut.channel_id NULLS FIRST,
              ut.created_at
            """,
            (user_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def _get_branch(tenant_id: int, branch_id: int) -> Optional[dict[str, Any]]:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(
            """
            SELECT
              id_empresa,
              id_filial,
              nome AS branch_name,
              is_active AS branch_is_enabled,
              valid_from AS branch_valid_from,
              valid_until AS branch_valid_until,
              blocked_reason AS branch_blocked_reason
            FROM auth.filiais
            WHERE id_empresa = %s AND id_filial = %s
            """,
            (tenant_id, branch_id),
        ).fetchone()
        return dict(row) if row else None


def _get_tenant_scope_row(tenant_id: int) -> Optional[dict[str, Any]]:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(
            """
            SELECT
              id_empresa,
              nome AS tenant_name,
              is_active AS tenant_is_enabled,
              status AS tenant_status,
              valid_from AS tenant_valid_from,
              valid_until AS tenant_valid_until,
              billing_status AS tenant_billing_status,
              grace_until AS tenant_grace_until,
              channel_id AS tenant_channel_id
            FROM app.tenants
            WHERE id_empresa = %s
            """,
            (tenant_id,),
        ).fetchone()
        return dict(row) if row else None


def _all_active_tenant_ids() -> list[int]:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        rows = conn.execute(
            """
            SELECT id_empresa
            FROM app.tenants
            WHERE is_active = true
            ORDER BY id_empresa
            """
        ).fetchall()
    return [int(row["id_empresa"]) for row in rows if row.get("id_empresa") is not None]


def _date_key_to_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    numeric = int(value)
    if numeric <= 0:
        return None
    year = numeric // 10000
    month = (numeric // 100) % 100
    day = numeric % 100
    return date(year, month, day)


def _channel_active_tenant_ids(channel_ids: list[int] | None = None) -> list[int]:
    if not channel_ids:
        return []

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        rows = conn.execute(
            """
            SELECT id_empresa, status
            FROM app.tenants
            WHERE is_active = true
              AND channel_id = ANY(%s)
            ORDER BY id_empresa
            """,
            (channel_ids,),
        ).fetchall()
    return [
        int(row["id_empresa"])
        for row in rows
        if row.get("id_empresa") is not None and tenant_status_allows_login(row.get("status"))
    ]


def _list_active_product_companies(
    tenant_ids: list[int] | None = None,
    channel_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    where_ids: list[str] = []
    params: list[Any] = []
    if tenant_ids:
        where_ids.append("id_empresa = ANY(%s)")
        params.append(tenant_ids)
    if channel_ids:
        where_ids.append("channel_id = ANY(%s)")
        params.append(channel_ids)

    where_sql = ""
    if where_ids:
        where_sql = "AND " + " AND ".join(where_ids)

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        rows = conn.execute(
            f"""
            SELECT
              id_empresa,
              nome AS tenant_name,
              status AS tenant_status,
              billing_status AS tenant_billing_status
            FROM app.tenants
            WHERE is_active = true
              {where_sql}
            ORDER BY id_empresa
            """,
            params,
        ).fetchall()
    return [
        {
            "id_empresa": int(row["id_empresa"]),
            "tenant_name": row.get("tenant_name"),
            "tenant_status": row.get("tenant_status"),
            "tenant_billing_status": row.get("tenant_billing_status"),
        }
        for row in rows
        if row.get("id_empresa") is not None
    ]


def _load_product_scope_defaults(tenant_id: int, branch_id: int | None) -> dict[str, Any]:
    cache_key = (int(tenant_id), int(branch_id) if branch_id is not None else None)
    now_monotonic = time.monotonic()
    cached = _product_scope_cache.get(cache_key)
    if cached and (now_monotonic - cached[0]) <= PRODUCT_SCOPE_CACHE_TTL_SECONDS:
        return dict(cached[1])

    where_filial = " AND id_filial = %s " if branch_id is not None else ""
    current_date = business_today(tenant_id)
    current_date_key = int(current_date.strftime("%Y%m%d"))
    branch_params: list[Any] = [] if branch_id is None else [branch_id]
    latest_params: list[Any] = [tenant_id] + branch_params
    finance_params: list[Any] = [tenant_id, current_date_key] + branch_params
    cash_params: list[Any] = [tenant_id, current_date_key] + branch_params

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        tenant_row = conn.execute(
            """
            SELECT default_product_scope_days
            FROM app.tenants
            WHERE id_empresa = %s
            """,
            (tenant_id,),
        ).fetchone()
        latest_row = conn.execute(
            f"""
            WITH candidates AS (
              SELECT
                MAX(data_key) AS latest_data_key,
                'fact_venda' AS source,
                10 AS priority
              FROM dw.fact_venda
              WHERE id_empresa = %s
                AND data_key IS NOT NULL
                {where_filial}
              UNION ALL
              SELECT
                MAX(data_key) AS latest_data_key,
                'fact_comprovante' AS source,
                20 AS priority
              FROM dw.fact_comprovante
              WHERE id_empresa = %s
                AND data_key IS NOT NULL
                {where_filial}
              UNION ALL
              SELECT
                MAX(data_key) AS latest_data_key,
                'fact_pagamento_comprovante' AS source,
                30 AS priority
              FROM dw.fact_pagamento_comprovante
              WHERE id_empresa = %s
                AND data_key IS NOT NULL
                {where_filial}
              UNION ALL
              SELECT
                MAX(COALESCE(data_key_pgto, data_key_venc, data_key_emissao)) AS latest_data_key,
                'fact_financeiro' AS source,
                40 AS priority
              FROM dw.fact_financeiro
              WHERE id_empresa = %s
                AND COALESCE(data_key_pgto, data_key_venc, data_key_emissao) IS NOT NULL
                AND COALESCE(data_key_pgto, data_key_venc, data_key_emissao) <= %s
                {where_filial}
              UNION ALL
              SELECT
                MAX(COALESCE(data_key_fechamento, data_key_abertura)) AS latest_data_key,
                'fact_caixa_turno' AS source,
                50 AS priority
              FROM dw.fact_caixa_turno
              WHERE id_empresa = %s
                AND COALESCE(data_key_fechamento, data_key_abertura) IS NOT NULL
                AND COALESCE(data_key_fechamento, data_key_abertura) <= %s
                {where_filial}
            )
            SELECT
              latest_data_key,
              source
            FROM candidates
            WHERE latest_data_key IS NOT NULL
            ORDER BY latest_data_key DESC, priority
            LIMIT 1
            """,
            latest_params
            + latest_params
            + latest_params
            + finance_params
            + cash_params,
        ).fetchone()

    default_days = int((tenant_row or {}).get("default_product_scope_days") or 1)
    latest_dt_ref = _date_key_to_date(latest_row.get("latest_data_key")) if latest_row else None
    result = {
        "default_product_scope_days": max(default_days, 1),
        "latest_dt_ref": latest_dt_ref or current_date,
        "current_date": current_date,
        "has_operational_data": latest_dt_ref is not None,
        "latest_source": latest_row.get("source") if latest_row else None,
    }
    _product_scope_cache[cache_key] = (now_monotonic, dict(result))
    return result


def _build_default_product_scope(tenant_id: int, branch_id: int | None) -> dict[str, Any]:
    scope_defaults = _load_product_scope_defaults(tenant_id, branch_id)
    dt_fim = scope_defaults["current_date"]
    default_days = int(scope_defaults["default_product_scope_days"])
    dt_ini = dt_fim - timedelta(days=max(default_days - 1, 0))
    branch_ids = [int(branch_id)] if branch_id is not None else []
    return {
        "id_empresa": tenant_id,
        "id_filial": branch_id,
        "id_filiais": branch_ids,
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": dt_fim.isoformat(),
        "dt_ref": scope_defaults["current_date"].isoformat(),
        "days": default_days,
        "source": "business_today_default",
        "latest_operational_dt": scope_defaults["latest_dt_ref"].isoformat(),
        "server_today": scope_defaults["current_date"].isoformat(),
        "business_today": scope_defaults["current_date"].isoformat(),
        "business_timezone": business_timezone_name(tenant_id),
        "latest_source": scope_defaults.get("latest_source"),
    }


def _build_dashboard_home_path(scope: dict[str, Any], include_dt_ref: bool = False) -> str:
    params: list[tuple[str, str]] = [
        ("dt_ini", str(scope["dt_ini"])),
        ("dt_fim", str(scope["dt_fim"])),
        ("id_empresa", str(scope["id_empresa"])),
    ]
    if include_dt_ref and scope.get("dt_ref"):
        params.append(("dt_ref", str(scope["dt_ref"])))

    branch_ids = [str(value) for value in (scope.get("id_filiais") or []) if value is not None]
    if scope.get("id_filial") is not None:
        params.append(("id_filial", str(scope["id_filial"])))
    elif len(branch_ids) == 1:
        params.append(("id_filial", branch_ids[0]))
    else:
        params.extend(("id_filiais", branch_id) for branch_id in branch_ids)

    return f"/dashboard?{urlencode(params, doseq=True)}"


def _record_failed_login(user_id: str) -> None:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(
            f"""
            UPDATE auth.users
            SET
              failed_login_count = COALESCE(failed_login_count, 0) + 1,
              locked_until = CASE
                WHEN COALESCE(failed_login_count, 0) + 1 >= {LOCK_AFTER_FAILURES}
                  THEN now() + interval '{LOCK_WINDOW_MINUTES} minutes'
                ELSE locked_until
              END,
              updated_at = now()
            WHERE id = %s::uuid
            """,
            (user_id,),
        )
        conn.commit()


def _record_successful_login(user_id: str) -> None:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        conn.execute(
            """
            UPDATE auth.users
            SET
              last_login_at = now(),
              failed_login_count = 0,
              locked_until = NULL,
              updated_at = now()
            WHERE id = %s::uuid
            """,
            (user_id,),
        )
        conn.commit()


def _user_now() -> tuple[date, datetime]:
    now = datetime.now(timezone.utc)
    return now.date(), now


def _assert_user_enabled(user: dict[str, Any], today: date, now: datetime) -> None:
    if bool(user.get("locked_until")) and user["locked_until"] > now:
        raise AuthError(423, "user_locked", "Usuário temporariamente bloqueado.")
    if not bool(user.get("is_active")):
        raise AuthError(403, "user_disabled", "Usuário não habilitado.")
    if not is_date_in_window(today, user.get("valid_from"), user.get("valid_until")):
        raise AuthError(403, "user_out_of_validity", "Usuário fora da vigência.")


def _access_row_is_valid_now(row: dict[str, Any], today: date) -> bool:
    if not bool(row.get("is_enabled", True)):
        return False
    return is_date_in_window(today, row.get("valid_from"), row.get("valid_until"))


def _preferred_access_rows(user_role: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def sort_key(row: dict[str, Any]) -> tuple[int, int, int, int]:
        company_level = 0 if row.get("id_filial") is None else 1
        return (
            role_priority(row.get("role")),
            int(row.get("id_empresa") or 0),
            company_level,
            int(row.get("id_filial") or 0),
        )

    filtered = [row for row in rows if normalize_role(row.get("role")) == user_role]
    return sorted(filtered, key=sort_key)


def _assert_channel_scope(row: dict[str, Any]) -> None:
    if not row.get("channel_id"):
        raise AuthError(403, "channel_scope_missing", "Usuário sem canal vinculado.")
    if row.get("channel_is_enabled") is False:
        raise AuthError(403, "channel_disabled", "Canal desabilitado.")


def _assert_tenant_scope(
    access_row: dict[str, Any],
    selected_branch_id: int | None,
    today: date,
    allow_internal_override: bool,
) -> tuple[int, int | None, list[str]]:
    tenant_id = access_row.get("id_empresa")
    if tenant_id is None:
        raise AuthError(403, "tenant_scope_missing", "Usuário sem empresa vinculada.")

    if not allow_internal_override:
        if access_row.get("tenant_is_enabled") is False:
            raise AuthError(403, "tenant_disabled", "Empresa desabilitada.")
        if not is_date_in_window(today, access_row.get("tenant_valid_from"), access_row.get("tenant_valid_until")):
            raise AuthError(403, "tenant_out_of_validity", "Empresa fora da vigência.")

        status = str(access_row.get("tenant_status") or "active")
        if not tenant_status_allows_login(status):
            if status == "suspended_total":
                raise AuthError(403, "tenant_suspended_total", "Empresa suspensa comercialmente.")
            if status == "cancelled":
                raise AuthError(403, "tenant_cancelled", "Empresa cancelada.")
            raise AuthError(403, "tenant_blocked", "Empresa bloqueada.")

    warnings: list[str] = []
    status = str(access_row.get("tenant_status") or "active")
    if tenant_status_is_warning(status):
        message = tenant_status_warning_message(status)
        if message:
            warnings.append(message)

    branch_id = selected_branch_id if selected_branch_id is not None else access_row.get("id_filial")
    if branch_id is None:
        return int(tenant_id), None, warnings

    branch = access_row if access_row.get("id_filial") == branch_id else _get_branch(int(tenant_id), int(branch_id))
    if not branch:
        raise AuthError(403, "branch_not_found", "Filial não encontrada.")
    if branch.get("branch_is_enabled") is False:
        raise AuthError(403, "branch_disabled", "Filial desabilitada.")
    if not is_date_in_window(today, branch.get("branch_valid_from"), branch.get("branch_valid_until")):
        raise AuthError(403, "branch_out_of_validity", "Filial fora da vigência.")

    return int(tenant_id), int(branch_id), warnings


def _select_channel_access(
    rows: list[dict[str, Any]],
    today: date,
    preferred_channel_id: int | None,
) -> dict[str, Any]:
    valid_rows = [row for row in rows if _access_row_is_valid_now(row, today)]
    if preferred_channel_id is not None:
        for row in valid_rows:
            if int(row.get("channel_id") or 0) == int(preferred_channel_id):
                _assert_channel_scope(row)
                return row
    if not valid_rows:
        raise AuthError(403, "access_unavailable", "Usuário sem vínculo de canal válido.")
    selected = valid_rows[0]
    _assert_channel_scope(selected)
    return selected


def _select_tenant_access(
    rows: list[dict[str, Any]],
    today: date,
    preferred_tenant_id: int | None,
    preferred_branch_id: int | None,
) -> tuple[dict[str, Any], int | None]:
    valid_rows = [row for row in rows if _access_row_is_valid_now(row, today)]
    if not valid_rows:
        raise AuthError(403, "access_unavailable", "Usuário sem vínculo de acesso válido.")

    if preferred_tenant_id is not None:
        tenant_rows = [row for row in valid_rows if int(row.get("id_empresa") or 0) == int(preferred_tenant_id)]
        if not tenant_rows:
            raise AuthError(403, "tenant_access_denied", "Acesso não permitido à empresa.")
    else:
        tenant_rows = valid_rows

    if preferred_branch_id is not None:
        exact_branch = next(
            (row for row in tenant_rows if row.get("id_filial") is not None and int(row["id_filial"]) == int(preferred_branch_id)),
            None,
        )
        if exact_branch:
            return exact_branch, int(preferred_branch_id)

        company_row = next((row for row in tenant_rows if row.get("id_filial") is None), None)
        if company_row:
            return company_row, int(preferred_branch_id)

        raise AuthError(403, "branch_access_denied", "Acesso não permitido à filial.")

    company_row = next((row for row in tenant_rows if row.get("id_filial") is None), None)
    if company_row:
        return company_row, None
    return tenant_rows[0], tenant_rows[0].get("id_filial")


def _serialize_access_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "role": normalize_role(row.get("role")),
        "channel_id": row.get("channel_id"),
        "channel_name": row.get("channel_name"),
        "id_empresa": row.get("id_empresa"),
        "tenant_name": row.get("tenant_name"),
        "tenant_status": row.get("tenant_status"),
        "tenant_billing_status": row.get("tenant_billing_status"),
        "id_filial": row.get("id_filial"),
        "branch_name": row.get("branch_name"),
        "is_enabled": bool(row.get("is_enabled", True)),
        "valid_from": row.get("valid_from"),
        "valid_until": row.get("valid_until"),
    }


def _build_session_context(
    user: dict[str, Any],
    access_rows: list[dict[str, Any]],
    preferred_tenant_id: int | None = None,
    preferred_branch_id: int | None = None,
    preferred_channel_id: int | None = None,
    include_default_scope: bool = False,
) -> dict[str, Any]:
    today, now = _user_now()
    sovereign_user = is_sovereign_email(user.get("email"))
    user_role = "platform_master" if sovereign_user else normalize_role(user.get("role"))
    if not user_role:
        raise AuthError(403, "user_role_missing", "Usuário sem papel configurado.")

    _assert_user_enabled(user, today, now)

    effective_access_rows = list(access_rows)
    if sovereign_user:
        effective_access_rows.append(
            {
                "role": "platform_master",
                "channel_id": None,
                "id_empresa": None,
                "id_filial": None,
                "is_enabled": True,
                "valid_from": user.get("valid_from"),
                "valid_until": user.get("valid_until"),
                "channel_name": None,
                "channel_is_enabled": True,
                "tenant_name": None,
                "tenant_is_enabled": True,
                "tenant_status": "active",
                "tenant_valid_from": None,
                "tenant_valid_until": None,
                "tenant_billing_status": None,
                "tenant_grace_until": None,
                "tenant_channel_id": None,
                "branch_name": None,
                "branch_is_enabled": True,
                "branch_valid_from": None,
                "branch_valid_until": None,
                "branch_blocked_reason": None,
            }
        )

    scoped_rows = _preferred_access_rows(user_role, effective_access_rows)
    if not scoped_rows:
        raise AuthError(403, "access_unavailable", "Usuário sem vínculo de acesso válido.")

    selected_tenant_id: int | None = None
    selected_branch_id: int | None = None
    selected_channel_id: int | None = None
    warnings: list[str] = []
    channel_tenant_ids: list[int] = []

    if user_role in {"platform_master", "platform_admin"}:
        global_rows = [row for row in scoped_rows if _access_row_is_valid_now(row, today)]
        selected = next((row for row in global_rows if row.get("id_empresa") is None and row.get("channel_id") is None), None)
        if not selected:
            raise AuthError(403, "access_unavailable", "Usuário interno sem vínculo global válido.")
    elif user_role == "product_global":
        global_rows = [row for row in scoped_rows if _access_row_is_valid_now(row, today)]
        selected = next((row for row in global_rows if row.get("id_empresa") is None and row.get("channel_id") is None), None)
        if not selected:
            raise AuthError(403, "access_unavailable", "Usuário global de produto sem vínculo global válido.")

        if preferred_tenant_id is not None:
            tenant_row = _get_tenant_scope_row(int(preferred_tenant_id))
            if not tenant_row:
                raise AuthError(403, "tenant_not_found", "Empresa não encontrada.")
            selected_tenant_id, selected_branch_id, warnings = _assert_tenant_scope(
                tenant_row,
                preferred_branch_id,
                today,
                allow_internal_override=False,
            )
    elif user_role == "channel_admin":
        selected = _select_channel_access(scoped_rows, today, preferred_channel_id)
        selected_channel_id = int(selected.get("channel_id"))
        channel_tenant_ids = _channel_active_tenant_ids([selected_channel_id])
        if preferred_tenant_id is not None:
            if int(preferred_tenant_id) not in set(channel_tenant_ids):
                raise AuthError(403, "tenant_access_denied", "Acesso não permitido à empresa.")
            tenant_row = _get_tenant_scope_row(int(preferred_tenant_id))
            if not tenant_row:
                raise AuthError(403, "tenant_not_found", "Empresa não encontrada.")
            if tenant_row.get("tenant_channel_id") != selected_channel_id:
                raise AuthError(403, "tenant_access_denied", "Acesso não permitido à empresa.")
            selected_tenant_id, selected_branch_id, warnings = _assert_tenant_scope(
                tenant_row,
                preferred_branch_id,
                today,
                allow_internal_override=False,
            )
    else:
        selected, selected_branch_id = _select_tenant_access(
            scoped_rows,
            today,
            preferred_tenant_id=preferred_tenant_id,
            preferred_branch_id=preferred_branch_id,
        )
        allow_internal_override = user_role in {"platform_master", "platform_admin"}
        selected_tenant_id, selected_branch_id, warnings = _assert_tenant_scope(
            selected,
            selected_branch_id,
            today,
            allow_internal_override=allow_internal_override,
        )

    analytics_role = analytics_role_for_user_role(user_role)
    product_readonly = is_product_readonly_role(user_role) or (
        selected.get("tenant_status") == "suspended_readonly"
        if user_role not in {"platform_master", "platform_admin"}
        else False
    )
    tenant_ids = (
        _all_active_tenant_ids()
        if user_role in {"platform_master", "product_global"}
        else channel_tenant_ids
        if user_role == "channel_admin"
        else sorted(
            {
                int(row["id_empresa"])
                for row in scoped_rows
                if row.get("id_empresa") is not None and _access_row_is_valid_now(row, today)
            }
        )
    )
    product_companies = (
        _list_active_product_companies(
            tenant_ids if user_role in {"product_global", "channel_admin"} else None,
        )
        if user_role in {"platform_master", "product_global", "channel_admin"}
        else [
            {
                "id_empresa": int(row["id_empresa"]),
                "tenant_name": row.get("tenant_name"),
                "tenant_status": row.get("tenant_status"),
                "tenant_billing_status": row.get("tenant_billing_status"),
            }
            for row in scoped_rows
            if row.get("id_empresa") is not None and _access_row_is_valid_now(row, today)
        ]
    )

    product_access_enabled = can_access_product(user_role) and (
        user_role != "channel_admin" or bool(tenant_ids)
    )

    product_scope_tenant = selected_tenant_id
    product_scope_branch = selected_branch_id
    if product_scope_tenant is None and product_access_enabled:
        product_scope_tenant = tenant_ids[0] if tenant_ids else None
        product_scope_branch = None

    default_scope = None
    home_path = (
        "/dashboard"
        if product_access_enabled
        else "/platform"
        if can_access_platform(user_role)
        else "/dashboard"
    )
    if include_default_scope and product_access_enabled and product_scope_tenant is not None:
        default_scope = _build_default_product_scope(product_scope_tenant, product_scope_branch)
        home_path = _build_dashboard_home_path(default_scope, include_dt_ref=True)

    return {
        "sub": str(user["id"]),
        "email": user["email"],
        "username": user.get("username"),
        "name": user.get("nome") or user["email"],
        "user_role": user_role,
        "role": analytics_role or user_role,
        "analytics_role": analytics_role,
        "role_label": role_label(user_role),
        "id_empresa": selected_tenant_id,
        "id_filial": selected_branch_id,
        "channel_id": selected_channel_id,
        "must_change_password": bool(user.get("must_change_password")),
        "last_login_at": user.get("last_login_at"),
        "tenant_status": selected.get("tenant_status"),
        "messages": warnings,
        "access": {
            "platform": can_access_platform(user_role),
            "platform_operations": can_manage_platform_operations(user_role),
            "platform_finance": can_manage_platform_finance(user_role),
            "platform_superuser": sovereign_user,
            "product": product_access_enabled,
            "product_readonly": product_readonly,
        },
        "server_today": today.isoformat(),
        "default_scope": default_scope,
        "home_path": home_path,
        "accesses": [_serialize_access_row(row) for row in scoped_rows if _access_row_is_valid_now(row, today)],
        "channel_ids": sorted(
            {
                int(row["channel_id"])
                for row in scoped_rows
                if row.get("channel_id") is not None and _access_row_is_valid_now(row, today)
            }
        ),
        "tenant_ids": tenant_ids,
        "product_companies": product_companies,
    }


def verify_login(
    identifier: str,
    password: str,
    id_empresa: int | None = None,
    id_filial: int | None = None,
    include_default_scope: bool = True,
) -> dict[str, Any]:
    user = get_user_by_identifier(identifier)
    if not user:
        verify_password(password, DUMMY_PASSWORD_HASH)
        raise AuthError(401, "invalid_credentials", "Credenciais inválidas.")

    if not verify_password(password, user["password_hash"]):
        _record_failed_login(str(user["id"]))
        raise AuthError(401, "invalid_credentials", "Credenciais inválidas.")

    session = _build_session_context(
        user,
        access_rows=_list_user_access_rows(str(user["id"])),
        preferred_tenant_id=id_empresa,
        preferred_branch_id=id_filial,
        include_default_scope=include_default_scope,
    )
    _record_successful_login(str(user["id"]))
    session["last_login_at"] = datetime.now(timezone.utc)
    return session


def get_session_context(
    user_id: str,
    id_empresa: int | None = None,
    id_filial: int | None = None,
    channel_id: int | None = None,
    include_default_scope: bool = False,
) -> dict[str, Any]:
    user = get_user_by_id(user_id)
    if not user:
        raise AuthError(401, "invalid_session", "Sessão inválida.")

    return _build_session_context(
        user,
        access_rows=_list_user_access_rows(str(user["id"])),
        preferred_tenant_id=id_empresa,
        preferred_branch_id=id_filial,
        preferred_channel_id=channel_id,
        include_default_scope=include_default_scope,
    )


def assert_platform_access(claims: dict[str, Any]) -> None:
    if not bool((claims.get("access") or {}).get("platform")):
        raise AuthError(403, "platform_forbidden", "Acesso interno não permitido.")


def assert_platform_operations_access(claims: dict[str, Any]) -> None:
    if not bool((claims.get("access") or {}).get("platform_operations")) and claims.get("user_role") != "channel_admin":
        raise AuthError(403, "platform_forbidden", "Ação operacional não permitida.")


def assert_platform_finance_access(claims: dict[str, Any]) -> None:
    if not bool((claims.get("access") or {}).get("platform_finance")):
        raise AuthError(403, "platform_finance_forbidden", "Acesso financeiro não permitido.")


def assert_product_write_allowed(claims: dict[str, Any]) -> None:
    access = claims.get("access") or {}
    if not bool(access.get("product")):
        raise AuthError(403, "product_forbidden", "Acesso ao produto não permitido.")
    if bool(access.get("product_readonly")):
        raise AuthError(403, "product_readonly", "Empresa em modo leitura.")
