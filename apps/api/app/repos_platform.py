from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from app.authz import normalize_role
from app.db import get_conn
from app.repos_auth import AuthError
from app.security import hash_password


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _month_start(value: date) -> date:
    return value.replace(day=1)


def _safe_month_date(year: int, month: int, day: int) -> date:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last_day = (next_month - timedelta(days=1)).day
    return date(year, month, min(max(day, 1), last_day))


def _add_months(value: date, months: int) -> date:
    month_index = (value.month - 1) + months
    year = value.year + month_index // 12
    month = (month_index % 12) + 1
    return date(year, month, 1)


def _serialize_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, default=str, ensure_ascii=False)


def _connect():
    return get_conn(role="MASTER", tenant_id=None, branch_id=None)


def _sync_tenant_identity(conn) -> None:
    conn.execute(
        """
        SELECT setval(
          pg_get_serial_sequence('app.tenants', 'id_empresa'),
          GREATEST(COALESCE((SELECT MAX(id_empresa) FROM app.tenants), 0), 1),
          true
        )
        """
    )


def _require_platform_access(claims: dict[str, Any]) -> None:
    if not bool((claims.get("access") or {}).get("platform")):
        raise AuthError(403, "platform_forbidden", "Acesso interno não permitido.")


def _require_platform_operations(claims: dict[str, Any]) -> None:
    role = normalize_role(claims.get("user_role"))
    if role in {"platform_master", "platform_admin", "channel_admin"}:
        return
    raise AuthError(403, "platform_forbidden", "Ação operacional não permitida.")


def _require_platform_master(claims: dict[str, Any]) -> None:
    if normalize_role(claims.get("user_role")) != "platform_master":
        raise AuthError(403, "platform_finance_forbidden", "Acesso financeiro não permitido.")


def _company_visibility_clause(claims: dict[str, Any]) -> tuple[str, list[Any]]:
    role = normalize_role(claims.get("user_role"))
    if role in {"platform_master", "platform_admin"}:
        return "", []
    if role == "channel_admin":
        channel_ids = list(claims.get("channel_ids") or [])
        if not channel_ids:
            return " AND 1 = 0 ", []
        return " AND t.channel_id = ANY(%s) ", [channel_ids]
    raise AuthError(403, "platform_forbidden", "Acesso interno não permitido.")


def _load_company_row(tenant_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = _load_company_row_tx(conn, tenant_id)
        return dict(row) if row else None


def _load_company_row_tx(conn, tenant_id: int):
    return conn.execute(
        """
        SELECT
          t.id_empresa,
          t.nome,
          t.cnpj,
          t.is_active,
          t.status,
          t.valid_from,
          t.valid_until,
          t.billing_status,
          t.grace_until,
          t.suspended_reason,
          t.suspended_at,
          t.reactivated_at,
          t.channel_id,
          t.plan_name,
          t.monthly_amount,
          t.billing_day,
          t.issue_day,
          t.created_at,
          t.updated_at,
          c.name AS channel_name
        FROM app.tenants t
        LEFT JOIN app.channels c
          ON c.id = t.channel_id
        WHERE t.id_empresa = %s
        """,
        (tenant_id,),
    ).fetchone()


def _assert_company_visible(claims: dict[str, Any], tenant_id: int) -> dict[str, Any]:
    _require_platform_access(claims)
    row = _load_company_row(tenant_id)
    if not row:
        raise AuthError(404, "tenant_not_found", "Empresa não encontrada.")

    role = normalize_role(claims.get("user_role"))
    if role == "channel_admin":
        if row.get("channel_id") not in set(claims.get("channel_ids") or []):
            raise AuthError(403, "tenant_access_denied", "Acesso não permitido à empresa.")
    return row


def _assert_company_mutable(claims: dict[str, Any], tenant_id: int) -> dict[str, Any]:
    row = _assert_company_visible(claims, tenant_id)
    _require_platform_operations(claims)
    return row


def _ensure_company_finance_permission(claims: dict[str, Any], payload: dict[str, Any]) -> None:
    restricted = {
        "status",
        "billing_status",
        "grace_until",
        "suspended_reason",
        "channel_id",
        "plan_name",
        "monthly_amount",
        "billing_day",
        "issue_day",
    }
    if restricted.intersection({k for k, v in payload.items() if v is not None}):
        _require_platform_master(claims)


def _load_company_branches(tenant_id: int) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
              id_empresa,
              id_filial,
              nome,
              cnpj,
              is_active,
              valid_from,
              valid_until,
              blocked_reason,
              created_at,
              updated_at
            FROM auth.filiais
            WHERE id_empresa = %s
            ORDER BY id_filial
            """,
            (tenant_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def _load_branch_row_tx(conn, tenant_id: int, branch_id: int):
    return conn.execute(
        """
        SELECT
          id_empresa,
          id_filial,
          nome,
          cnpj,
          is_active,
          valid_from,
          valid_until,
          blocked_reason,
          created_at,
          updated_at
        FROM auth.filiais
        WHERE id_empresa = %s
          AND id_filial = %s
        """,
        (tenant_id, branch_id),
    ).fetchone()


def _load_user_rows() -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
              u.id,
              u.nome,
              u.email,
              u.role,
              u.is_active,
              u.valid_from,
              u.valid_until,
              u.must_change_password,
              u.last_login_at,
              u.failed_login_count,
              u.locked_until,
              u.created_at,
              u.updated_at,
              n.telegram_chat_id,
              n.telegram_username,
              n.telegram_enabled,
              n.email AS contact_email,
              n.phone AS contact_phone
            FROM auth.users u
            LEFT JOIN app.user_notification_settings n
              ON n.user_id = u.id
            ORDER BY u.created_at DESC, u.email
            """
        ).fetchall()
        return [dict(row) for row in rows]


def _load_user_access_rows(user_ids: list[str] | None = None) -> list[dict[str, Any]]:
    params: list[Any] = []
    where = ""
    if user_ids:
        where = "WHERE ut.user_id = ANY(%s::uuid[])"
        params.append(user_ids)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
              ut.user_id::text AS user_id,
              ut.role,
              ut.channel_id,
              ut.id_empresa,
              ut.id_filial,
              ut.is_enabled,
              ut.valid_from,
              ut.valid_until,
              ch.name AS channel_name,
              t.nome AS tenant_name,
              t.channel_id AS tenant_channel_id,
              f.nome AS branch_name
            FROM auth.user_tenants ut
            LEFT JOIN app.channels ch
              ON ch.id = ut.channel_id
            LEFT JOIN app.tenants t
              ON t.id_empresa = ut.id_empresa
            LEFT JOIN auth.filiais f
              ON f.id_empresa = ut.id_empresa
             AND f.id_filial = ut.id_filial
            {where}
            ORDER BY ut.user_id, ut.id_empresa NULLS FIRST, ut.id_filial NULLS FIRST
            """,
            params,
        ).fetchall()
        return [dict(row) for row in rows]


def _user_visible_to_platform(claims: dict[str, Any], user: dict[str, Any], accesses: list[dict[str, Any]]) -> bool:
    role = normalize_role(claims.get("user_role"))
    if role in {"platform_master", "platform_admin"}:
        return True
    if role != "channel_admin":
        return False
    visible_channels = set(claims.get("channel_ids") or [])
    for access in accesses:
        if access.get("channel_id") in visible_channels:
            return True
        tenant_channel_id = access.get("tenant_channel_id")
        if tenant_channel_id in visible_channels:
            return True
    return False


def _group_user_accesses(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["user_id"]), []).append(
            {
                "role": row.get("role"),
                "channel_id": row.get("channel_id"),
                "channel_name": row.get("channel_name"),
                "id_empresa": row.get("id_empresa"),
                "tenant_name": row.get("tenant_name"),
                "id_filial": row.get("id_filial"),
                "branch_name": row.get("branch_name"),
                "is_enabled": bool(row.get("is_enabled", True)),
                "valid_from": row.get("valid_from"),
                "valid_until": row.get("valid_until"),
            }
        )
    return grouped


def _ensure_user_contacts_row(conn, user_id: str) -> None:
    conn.execute(
        """
        INSERT INTO app.user_notification_settings (user_id)
        VALUES (%s::uuid)
        ON CONFLICT (user_id) DO NOTHING
        """,
        (user_id,),
    )


def _audit(conn, claims: dict[str, Any], action: str, entity_type: str, entity_id: str, old_values: Any, new_values: Any, ip: str | None) -> None:
    conn.execute(
        """
        INSERT INTO audit.audit_log (
          actor_user_id,
          actor_role,
          action,
          entity_type,
          entity_id,
          old_values,
          new_values,
          created_at,
          ip
        )
        VALUES (%s::uuid, %s, %s, %s, %s, %s::jsonb, %s::jsonb, now(), %s)
        """,
        (
            claims.get("sub"),
            claims.get("user_role"),
            action,
            entity_type,
            entity_id,
            _serialize_json(old_values),
            _serialize_json(new_values),
            ip,
        ),
    )


def list_companies(
    claims: dict[str, Any],
    search: str | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    _require_platform_access(claims)
    visibility_sql, visibility_params = _company_visibility_clause(claims)
    params: list[Any] = []
    filters = ""
    if search:
        filters += " AND (t.nome ILIKE %s OR COALESCE(t.cnpj, '') ILIKE %s) "
        like = f"%{search.strip()}%"
        params.extend([like, like])
    if status:
        filters += " AND t.status = %s "
        params.append(status)

    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
              t.id_empresa,
              t.nome,
              t.cnpj,
              t.is_active,
              t.status,
              t.valid_from,
              t.valid_until,
              t.billing_status,
              t.grace_until,
              t.channel_id,
              c.name AS channel_name,
              t.plan_name,
              t.monthly_amount,
              t.billing_day,
              t.issue_day,
              t.created_at,
              t.updated_at
            FROM app.tenants t
            LEFT JOIN app.channels c
              ON c.id = t.channel_id
            WHERE 1 = 1
            {visibility_sql}
            {filters}
            ORDER BY t.id_empresa DESC
            LIMIT %s OFFSET %s
            """,
            visibility_params + params + [limit, offset],
        ).fetchall()
        total_row = conn.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM app.tenants t
            WHERE 1 = 1
            {visibility_sql}
            {filters}
            """,
            visibility_params + params,
        ).fetchone()

    return {"items": [dict(row) for row in rows], "total": int(total_row["total"] or 0)}


def get_company_detail(claims: dict[str, Any], tenant_id: int) -> dict[str, Any]:
    company = _assert_company_visible(claims, tenant_id)
    company["branches"] = _load_company_branches(tenant_id)
    company["users"] = list_users(claims, tenant_id=tenant_id, limit=500, offset=0)["items"]
    role = normalize_role(claims.get("user_role"))
    if role in {"platform_master", "channel_admin"}:
        contracts = list_contracts(claims, tenant_id=tenant_id, limit=10, offset=0, allow_non_master=True)
        company["contract"] = (contracts["items"] or [None])[0]
        company["contracts"] = contracts["items"]
    else:
        company["contract"] = None
        company["contracts"] = []
    company["notification_subscriptions"] = list_notification_subscriptions(
        claims,
        tenant_id=tenant_id,
        limit=200,
        offset=0,
    )["items"]
    company["audit"] = list_audit(claims, tenant_id=tenant_id, limit=30) if role == "platform_master" else []
    return company


def upsert_company(
    claims: dict[str, Any],
    payload: dict[str, Any],
    ip: str | None,
    tenant_id: int | None = None,
) -> dict[str, Any]:
    _require_platform_operations(claims)
    _ensure_company_finance_permission(claims, payload)

    role = normalize_role(claims.get("user_role"))
    if role == "channel_admin":
        allowed_channels = list(claims.get("channel_ids") or [])
        if not allowed_channels:
            raise AuthError(403, "channel_scope_missing", "Usuário sem canal vinculado.")
        channel_id = payload.get("channel_id") or allowed_channels[0]
        if channel_id not in allowed_channels:
            raise AuthError(403, "channel_access_denied", "Canal não permitido.")
        payload["channel_id"] = channel_id
        for field in ("status", "billing_status", "grace_until", "suspended_reason", "plan_name", "monthly_amount", "billing_day", "issue_day"):
            if payload.get(field) is not None:
                raise AuthError(403, "platform_finance_forbidden", "Acesso financeiro não permitido.")

    with _connect() as conn:
        previous = _load_company_row(tenant_id) if tenant_id is not None else None
        if tenant_id is None:
            _sync_tenant_identity(conn)
            row = conn.execute(
                """
                INSERT INTO app.tenants (
                  nome,
                  cnpj,
                  is_active,
                  status,
                  valid_from,
                  valid_until,
                  billing_status,
                  grace_until,
                  suspended_reason,
                  channel_id,
                  plan_name,
                  monthly_amount,
                  billing_day,
                  issue_day,
                  updated_at
                )
                VALUES (
                  %s, %s, %s, COALESCE(%s, 'active'), COALESCE(%s, CURRENT_DATE), %s,
                  COALESCE(%s, 'current'), %s, %s, %s, %s, %s, %s, %s, now()
                )
                RETURNING id_empresa
                """,
                (
                    payload["nome"],
                    payload.get("cnpj"),
                    bool(payload.get("is_enabled", True)),
                    payload.get("status"),
                    payload.get("valid_from"),
                    payload.get("valid_until"),
                    payload.get("billing_status"),
                    payload.get("grace_until"),
                    payload.get("suspended_reason"),
                    payload.get("channel_id"),
                    payload.get("plan_name"),
                    payload.get("monthly_amount"),
                    payload.get("billing_day"),
                    payload.get("issue_day"),
                ),
            ).fetchone()
            tenant_id = int(row["id_empresa"])
            entity = dict(_load_company_row_tx(conn, tenant_id))
            _audit(conn, claims, "tenant.create", "tenant", str(tenant_id), None, entity, ip)
        else:
            _assert_company_mutable(claims, tenant_id)
            conn.execute(
                """
                UPDATE app.tenants
                SET
                  nome = %s,
                  cnpj = %s,
                  is_active = %s,
                  valid_from = COALESCE(%s, valid_from),
                  valid_until = %s,
                  status = COALESCE(%s, status),
                  billing_status = COALESCE(%s, billing_status),
                  grace_until = %s,
                  suspended_reason = %s,
                  suspended_at = CASE
                    WHEN %s IS NOT NULL AND %s <> COALESCE(status, '')
                      AND %s IN ('suspended_readonly', 'suspended_total')
                      THEN now()
                    ELSE suspended_at
                  END,
                  reactivated_at = CASE
                    WHEN %s IS NOT NULL AND %s IN ('active', 'trial', 'overdue', 'grace')
                      AND COALESCE(status, '') IN ('suspended_readonly', 'suspended_total')
                      THEN now()
                    ELSE reactivated_at
                  END,
                  channel_id = COALESCE(%s, channel_id),
                  plan_name = COALESCE(%s, plan_name),
                  monthly_amount = COALESCE(%s, monthly_amount),
                  billing_day = COALESCE(%s, billing_day),
                  issue_day = COALESCE(%s, issue_day),
                  updated_at = now()
                WHERE id_empresa = %s
                """,
                (
                    payload["nome"],
                    payload.get("cnpj"),
                    bool(payload.get("is_enabled", True)),
                    payload.get("valid_from"),
                    payload.get("valid_until"),
                    payload.get("status"),
                    payload.get("billing_status"),
                    payload.get("grace_until"),
                    payload.get("suspended_reason"),
                    payload.get("status"),
                    payload.get("status"),
                    payload.get("status"),
                    payload.get("status"),
                    payload.get("status"),
                    payload.get("channel_id"),
                    payload.get("plan_name"),
                    payload.get("monthly_amount"),
                    payload.get("billing_day"),
                    payload.get("issue_day"),
                    tenant_id,
                ),
            )
            entity = dict(_load_company_row_tx(conn, tenant_id))
            action = "tenant.suspend" if payload.get("status") in {"suspended_readonly", "suspended_total"} else "tenant.update"
            if payload.get("status") in {"active", "trial", "overdue", "grace"} and previous and previous.get("status") in {"suspended_readonly", "suspended_total"}:
                action = "tenant.reactivate"
            _audit(conn, claims, action, "tenant", str(tenant_id), previous, entity, ip)
        conn.commit()
    return entity


def upsert_branch(
    claims: dict[str, Any],
    tenant_id: int,
    payload: dict[str, Any],
    ip: str | None,
    branch_id: int | None = None,
) -> dict[str, Any]:
    _assert_company_mutable(claims, tenant_id)
    if branch_id is None:
        raise AuthError(
            409,
            "branch_sync_managed",
            "Filiais são sincronizadas da Xpert via ingest/ETL e não podem ser cadastradas manualmente.",
        )

    with _connect() as conn:
        previous = _load_branch_row_tx(conn, tenant_id, branch_id)
        if not previous:
            raise AuthError(404, "branch_not_found", "Filial não encontrada.")
        previous = dict(previous)

        conn.execute(
            """
            UPDATE auth.filiais
            SET
              nome = %s,
              cnpj = %s,
              is_active = %s,
              valid_from = %s,
              valid_until = %s,
              blocked_reason = %s,
              updated_at = now()
            WHERE id_empresa = %s
              AND id_filial = %s
            """,
            (
                payload["nome"],
                payload.get("cnpj"),
                bool(payload.get("is_enabled", True)),
                payload.get("valid_from"),
                payload.get("valid_until"),
                payload.get("blocked_reason"),
                tenant_id,
                branch_id,
            ),
        )

        current = _load_branch_row_tx(conn, tenant_id, branch_id)
        entity = dict(current)
        _audit(conn, claims, "branch.update", "branch", f"{tenant_id}:{branch_id}", previous, entity, ip)
        conn.commit()
    return entity


def list_users(
    claims: dict[str, Any],
    tenant_id: int | None = None,
    search: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    _require_platform_access(claims)
    users = _load_user_rows()
    access_map = _group_user_accesses(_load_user_access_rows([str(user["id"]) for user in users]))

    filtered: list[dict[str, Any]] = []
    for user in users:
        accesses = access_map.get(str(user["id"]), [])
        if not _user_visible_to_platform(claims, user, accesses):
            continue
        if tenant_id is not None and not any(access.get("id_empresa") == tenant_id for access in accesses):
            continue
        if search:
            search_value = search.lower().strip()
            haystack = " ".join(
                [
                    str(user.get("nome") or ""),
                    str(user.get("email") or ""),
                    " ".join(str(access.get("tenant_name") or "") for access in accesses),
                ]
            ).lower()
            if search_value not in haystack:
                continue
        filtered.append(
            {
                **user,
                "id": str(user.get("id")),
                "is_enabled": bool(user.get("is_active", True)),
                "telegram_configured": bool(user.get("telegram_enabled") and user.get("telegram_chat_id")),
                "accesses": accesses,
            }
        )

    total = len(filtered)
    page = filtered[offset : offset + limit]
    return {"items": page, "total": total}


def _validate_user_management_role(current_claims: dict[str, Any], target_role: str) -> None:
    actor_role = normalize_role(current_claims.get("user_role"))
    target_role = normalize_role(target_role)
    if actor_role == "platform_master":
        return
    if actor_role == "platform_admin":
        if target_role in {"platform_master", "platform_admin"}:
            raise AuthError(403, "role_escalation_forbidden", "Papel interno não permitido.")
        return
    if actor_role == "channel_admin":
        if target_role not in {"tenant_admin", "tenant_manager", "tenant_viewer"}:
            raise AuthError(403, "role_escalation_forbidden", "Papel não permitido para canal.")
        return
    raise AuthError(403, "platform_forbidden", "Ação operacional não permitida.")


def _validate_access_payload(actor_claims: dict[str, Any], user_role: str, accesses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    role = normalize_role(user_role)
    normalized: list[dict[str, Any]] = []
    if not accesses:
        if role in {"platform_master", "platform_admin"}:
            return [{"role": role, "channel_id": None, "id_empresa": None, "id_filial": None, "is_enabled": True, "valid_from": None, "valid_until": None}]
        raise AuthError(422, "validation_error", "Usuário precisa de pelo menos um vínculo de acesso.")

    for access in accesses:
        access_role = normalize_role(access.get("role"))
        if access_role != role:
            raise AuthError(422, "validation_error", "Todos os vínculos devem usar o mesmo papel do usuário.")
        if role == "channel_admin":
            channel_ids = set(actor_claims.get("channel_ids") or [])
            if normalize_role(actor_claims.get("user_role")) == "platform_master":
                channel_ids = channel_ids or {access.get("channel_id")}
            if not access.get("channel_id"):
                raise AuthError(422, "validation_error", "channel_id é obrigatório para channel_admin.")
            if normalize_role(actor_claims.get("user_role")) == "channel_admin" and access.get("channel_id") not in channel_ids:
                raise AuthError(403, "channel_access_denied", "Canal não permitido.")
        if role in {"tenant_admin", "tenant_manager", "tenant_viewer"}:
            tenant_scope = access.get("id_empresa")
            if tenant_scope is None:
                raise AuthError(422, "validation_error", "id_empresa é obrigatório para perfis tenant.")
            if normalize_role(actor_claims.get("user_role")) == "channel_admin":
                company = _assert_company_visible(actor_claims, int(tenant_scope))
                if company.get("channel_id") not in set(actor_claims.get("channel_ids") or []):
                    raise AuthError(403, "tenant_access_denied", "Empresa não permitida para o canal.")
        normalized.append(access)

    return normalized


def upsert_user(
    claims: dict[str, Any],
    payload: dict[str, Any],
    ip: str | None,
    user_id: str | None = None,
) -> dict[str, Any]:
    _require_platform_operations(claims)
    _validate_user_management_role(claims, payload["role"])
    accesses = _validate_access_payload(claims, payload["role"], payload.get("accesses") or [])

    with _connect() as conn:
        previous = None
        if user_id:
            if not any(item["id"] == user_id for item in list_users(claims, limit=5000, offset=0)["items"]):
                raise AuthError(403, "user_access_denied", "Acesso não permitido ao usuário.")
            previous_row = conn.execute(
                "SELECT * FROM auth.users WHERE id = %s::uuid",
                (user_id,),
            ).fetchone()
            if not previous_row:
                raise AuthError(404, "user_not_found", "Usuário não encontrado.")
            previous = dict(previous_row)
            password_sql = ""
            params: list[Any] = [
                payload["nome"],
                payload["email"].lower(),
                payload["role"],
                bool(payload.get("is_enabled", True)),
                payload.get("valid_from"),
                payload.get("valid_until"),
                bool(payload.get("must_change_password", False)),
                payload.get("locked_until"),
                bool(payload.get("reset_failed_login", False)),
            ]
            if payload.get("password"):
                password_sql = ", password_hash = %s"
                params.append(hash_password(payload["password"]))
            params.append(user_id)
            conn.execute(
                f"""
                UPDATE auth.users
                SET
                  nome = %s,
                  email = %s,
                  role = %s,
                  is_active = %s,
                  valid_from = COALESCE(%s, valid_from),
                  valid_until = %s,
                  must_change_password = %s,
                  locked_until = %s,
                  failed_login_count = CASE WHEN %s THEN 0 ELSE failed_login_count END
                  {password_sql}
                WHERE id = %s::uuid
                """,
                params,
            )
        else:
            if not payload.get("password"):
                raise AuthError(422, "validation_error", "Senha é obrigatória para criar usuário.")
            created = conn.execute(
                """
                INSERT INTO auth.users (
                  nome,
                  email,
                  password_hash,
                  role,
                  is_active,
                  valid_from,
                  valid_until,
                  must_change_password
                )
                VALUES (%s, %s, %s, %s, %s, COALESCE(%s, CURRENT_DATE), %s, %s)
                RETURNING id
                """,
                (
                    payload["nome"],
                    payload["email"].lower(),
                    hash_password(payload["password"]),
                    payload["role"],
                    bool(payload.get("is_enabled", True)),
                    payload.get("valid_from"),
                    payload.get("valid_until"),
                    bool(payload.get("must_change_password", False)),
                ),
            ).fetchone()
            user_id = str(created["id"])

        conn.execute("DELETE FROM auth.user_tenants WHERE user_id = %s::uuid", (user_id,))
        for access in accesses:
            conn.execute(
                """
                INSERT INTO auth.user_tenants (
                  user_id,
                  role,
                  channel_id,
                  id_empresa,
                  id_filial,
                  is_enabled,
                  valid_from,
                  valid_until
                )
                VALUES (%s::uuid, %s, %s, %s, %s, %s, COALESCE(%s, CURRENT_DATE), %s)
                """,
                (
                    user_id,
                    payload["role"],
                    access.get("channel_id"),
                    access.get("id_empresa"),
                    access.get("id_filial"),
                    bool(access.get("is_enabled", True)),
                    access.get("valid_from"),
                    access.get("valid_until"),
                ),
            )

        _ensure_user_contacts_row(conn, user_id)
        current_user = conn.execute(
            """
            SELECT
              id::text AS id,
              nome,
              email,
              role,
              is_active,
              valid_from,
              valid_until,
              must_change_password,
              failed_login_count,
              locked_until,
              last_login_at
            FROM auth.users
            WHERE id = %s::uuid
            """,
            (user_id,),
        ).fetchone()
        current = dict(current_user)
        current["accesses"] = [
            {
                "role": payload["role"],
                "channel_id": access.get("channel_id"),
                "id_empresa": access.get("id_empresa"),
                "id_filial": access.get("id_filial"),
                "is_enabled": bool(access.get("is_enabled", True)),
                "valid_from": access.get("valid_from"),
                "valid_until": access.get("valid_until"),
            }
            for access in accesses
        ]
        _audit(conn, claims, "user.update" if previous else "user.create", "user", user_id, previous, current, ip)
        conn.commit()
    return current


def upsert_user_contacts(claims: dict[str, Any], user_id: str, payload: dict[str, Any], ip: str | None) -> dict[str, Any]:
    _require_platform_operations(claims)
    if not any(item["id"] == user_id for item in list_users(claims, limit=5000, offset=0)["items"]):
        raise AuthError(403, "user_access_denied", "Acesso não permitido ao usuário.")
    with _connect() as conn:
        _ensure_user_contacts_row(conn, user_id)
        previous = conn.execute(
            "SELECT * FROM app.user_notification_settings WHERE user_id = %s::uuid",
            (user_id,),
        ).fetchone()
        previous_dict = dict(previous) if previous else None
        conn.execute(
            """
            UPDATE app.user_notification_settings
            SET
              telegram_chat_id = %s,
              telegram_username = %s,
              telegram_enabled = %s,
              email = %s,
              phone = %s,
              updated_at = now()
            WHERE user_id = %s::uuid
            """,
            (
                payload.get("telegram_chat_id"),
                payload.get("telegram_username"),
                bool(payload.get("telegram_enabled", False)),
                payload.get("email"),
                payload.get("phone"),
                user_id,
            ),
        )
        current = conn.execute(
            "SELECT * FROM app.user_notification_settings WHERE user_id = %s::uuid",
            (user_id,),
        ).fetchone()
        current_dict = dict(current)
        _audit(conn, claims, "user.contacts.update", "user_contacts", user_id, previous_dict, current_dict, ip)
        conn.commit()
    return current_dict


def list_channels(claims: dict[str, Any], limit: int = 50, offset: int = 0) -> dict[str, Any]:
    _require_platform_master(claims)
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
              c.*,
              (
                SELECT COUNT(*)::int
                FROM app.tenants t
                WHERE t.channel_id = c.id
              ) AS companies_count
            FROM app.channels c
            ORDER BY c.created_at DESC, c.id DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS total FROM app.channels").fetchone()
    return {"items": [dict(row) for row in rows], "total": int(total["total"] or 0)}


def upsert_channel(claims: dict[str, Any], payload: dict[str, Any], ip: str | None, channel_id: int | None = None) -> dict[str, Any]:
    _require_platform_master(claims)
    with _connect() as conn:
        previous = None
        if channel_id is None:
            row = conn.execute(
                """
                INSERT INTO app.channels (name, contact_name, email, phone, is_enabled, notes)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    payload["name"],
                    payload.get("contact_name"),
                    payload.get("email"),
                    payload.get("phone"),
                    bool(payload.get("is_enabled", True)),
                    payload.get("notes"),
                ),
            ).fetchone()
            current = dict(row)
        else:
            previous = conn.execute("SELECT * FROM app.channels WHERE id = %s", (channel_id,)).fetchone()
            if not previous:
                raise AuthError(404, "channel_not_found", "Canal não encontrado.")
            previous = dict(previous)
            row = conn.execute(
                """
                UPDATE app.channels
                SET
                  name = %s,
                  contact_name = %s,
                  email = %s,
                  phone = %s,
                  is_enabled = %s,
                  notes = %s,
                  updated_at = now()
                WHERE id = %s
                RETURNING *
                """,
                (
                    payload["name"],
                    payload.get("contact_name"),
                    payload.get("email"),
                    payload.get("phone"),
                    bool(payload.get("is_enabled", True)),
                    payload.get("notes"),
                    channel_id,
                ),
            ).fetchone()
            current = dict(row)
        _audit(conn, claims, "channel.update" if previous else "channel.create", "channel", str(current["id"]), previous, current, ip)
        conn.commit()
    return current


def _sync_active_contract_summary(conn, tenant_id: int) -> None:
    active = conn.execute(
        """
        SELECT *
        FROM billing.contracts
        WHERE tenant_id = %s AND is_enabled = true
        ORDER BY start_date DESC, id DESC
        LIMIT 1
        """,
        (tenant_id,),
    ).fetchone()
    if active:
        conn.execute(
            """
            UPDATE app.tenants
            SET
              channel_id = %s,
              plan_name = %s,
              monthly_amount = %s,
              billing_day = %s,
              issue_day = %s,
              updated_at = now()
            WHERE id_empresa = %s
            """,
            (
                active["channel_id"],
                active["plan_name"],
                active["monthly_amount"],
                active["billing_day"],
                active["issue_day"],
                tenant_id,
            ),
        )
    else:
        conn.execute(
            """
            UPDATE app.tenants
            SET
              channel_id = NULL,
              plan_name = NULL,
              monthly_amount = NULL,
              billing_day = NULL,
              issue_day = NULL,
              updated_at = now()
            WHERE id_empresa = %s
            """,
            (tenant_id,),
        )


def _contract_identity_changed(previous: dict[str, Any], payload: dict[str, Any]) -> bool:
    tracked_fields = (
        "tenant_id",
        "channel_id",
        "plan_name",
        "monthly_amount",
        "billing_day",
        "issue_day",
        "start_date",
        "commission_first_year_pct",
        "commission_recurring_pct",
    )
    for field in tracked_fields:
        if previous.get(field) != payload.get(field):
            return True
    return False


def _close_active_contracts(
    conn,
    claims: dict[str, Any],
    tenant_id: int,
    new_start_date: date,
    exclude_id: int | None,
    ip: str | None,
) -> None:
    query = """
        SELECT *
        FROM billing.contracts
        WHERE tenant_id = %s
          AND is_enabled = true
    """
    params: list[Any] = [tenant_id]
    if exclude_id is not None:
        query += " AND id <> %s "
        params.append(exclude_id)
    query += " ORDER BY start_date DESC, id DESC "
    rows = conn.execute(query, params).fetchall()
    previous_end = new_start_date - timedelta(days=1)
    for row in rows:
        previous = dict(row)
        transition_end = previous_end if previous_end >= previous["start_date"] else previous["start_date"]
        end_date = previous.get("end_date")
        if end_date and end_date <= transition_end:
            if not previous.get("is_enabled"):
                continue
            next_state = {**previous, "is_enabled": False}
        else:
            next_state = {
                **previous,
                "is_enabled": False,
                "end_date": transition_end,
            }
        conn.execute(
            """
            UPDATE billing.contracts
            SET
              is_enabled = false,
              end_date = %s,
              updated_at = now()
            WHERE id = %s
            """,
            (next_state.get("end_date"), previous["id"]),
        )
        _audit(
            conn,
            claims,
            "contract.close",
            "contract",
            str(previous["id"]),
            previous,
            next_state,
            ip,
        )


def list_contracts(
    claims: dict[str, Any],
    tenant_id: int | None = None,
    limit: int = 50,
    offset: int = 0,
    allow_non_master: bool = False,
) -> dict[str, Any]:
    role = normalize_role(claims.get("user_role"))
    if not allow_non_master:
        if role != "platform_master":
            raise AuthError(403, "platform_finance_forbidden", "Acesso financeiro não permitido.")
    elif role not in {"platform_master", "channel_admin"}:
        raise AuthError(403, "platform_forbidden", "Acesso ao contrato não permitido.")

    params: list[Any] = []
    filters = ""
    if tenant_id is not None:
        _assert_company_visible(claims, tenant_id)
        filters += " AND c.tenant_id = %s "
        params.append(tenant_id)
    visibility_sql, visibility_params = _company_visibility_clause(claims)

    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
              c.*,
              t.nome AS tenant_name,
              ch.name AS channel_name
            FROM billing.contracts c
            JOIN app.tenants t
              ON t.id_empresa = c.tenant_id
            LEFT JOIN app.channels ch
              ON ch.id = c.channel_id
            WHERE 1 = 1
            {filters}
            {visibility_sql.replace('t.', 't.')}
            ORDER BY c.is_enabled DESC, c.start_date DESC, c.id DESC
            LIMIT %s OFFSET %s
            """,
            params + visibility_params + [limit, offset],
        ).fetchall()
        total = conn.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM billing.contracts c
            JOIN app.tenants t
              ON t.id_empresa = c.tenant_id
            WHERE 1 = 1
            {filters}
            {visibility_sql.replace('t.', 't.')}
            """,
            params + visibility_params,
        ).fetchone()
    return {"items": [dict(row) for row in rows], "total": int(total["total"] or 0)}


def upsert_contract(claims: dict[str, Any], payload: dict[str, Any], ip: str | None, contract_id: int | None = None) -> dict[str, Any]:
    _require_platform_master(claims)
    _assert_company_visible(claims, int(payload["tenant_id"]))
    with _connect() as conn:
        previous = None
        if contract_id is None:
            if bool(payload.get("is_enabled", True)):
                _close_active_contracts(
                    conn,
                    claims,
                    int(payload["tenant_id"]),
                    payload["start_date"],
                    exclude_id=None,
                    ip=ip,
                )
            row = conn.execute(
                """
                INSERT INTO billing.contracts (
                  tenant_id,
                  channel_id,
                  plan_name,
                  monthly_amount,
                  billing_day,
                  issue_day,
                  start_date,
                  end_date,
                  is_enabled,
                  commission_first_year_pct,
                  commission_recurring_pct,
                  notes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    payload["tenant_id"],
                    payload.get("channel_id"),
                    payload["plan_name"],
                    payload["monthly_amount"],
                    payload["billing_day"],
                    payload["issue_day"],
                    payload["start_date"],
                    payload.get("end_date"),
                    bool(payload.get("is_enabled", True)),
                    payload["commission_first_year_pct"],
                    payload["commission_recurring_pct"],
                    payload.get("notes"),
                ),
            ).fetchone()
            current = dict(row)
        else:
            previous = conn.execute("SELECT * FROM billing.contracts WHERE id = %s", (contract_id,)).fetchone()
            if not previous:
                raise AuthError(404, "contract_not_found", "Contrato não encontrado.")
            previous = dict(previous)
            if _contract_identity_changed(previous, payload):
                conn.execute(
                    """
                    UPDATE billing.contracts
                    SET
                      is_enabled = false,
                      end_date = %s,
                      updated_at = now()
                    WHERE id = %s
                    """,
                    (
                        (payload["start_date"] - timedelta(days=1))
                        if (payload["start_date"] - timedelta(days=1)) >= previous["start_date"]
                        else previous["start_date"],
                        contract_id,
                    ),
                )
                closed_previous = conn.execute(
                    "SELECT * FROM billing.contracts WHERE id = %s",
                    (contract_id,),
                ).fetchone()
                _audit(conn, claims, "contract.close", "contract", str(contract_id), previous, dict(closed_previous), ip)
                if bool(payload.get("is_enabled", True)):
                    _close_active_contracts(
                        conn,
                        claims,
                        int(payload["tenant_id"]),
                        payload["start_date"],
                        exclude_id=contract_id,
                        ip=ip,
                    )
                row = conn.execute(
                    """
                    INSERT INTO billing.contracts (
                      tenant_id,
                      channel_id,
                      plan_name,
                      monthly_amount,
                      billing_day,
                      issue_day,
                      start_date,
                      end_date,
                      is_enabled,
                      commission_first_year_pct,
                      commission_recurring_pct,
                      notes
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        payload["tenant_id"],
                        payload.get("channel_id"),
                        payload["plan_name"],
                        payload["monthly_amount"],
                        payload["billing_day"],
                        payload["issue_day"],
                        payload["start_date"],
                        payload.get("end_date"),
                        bool(payload.get("is_enabled", True)),
                        payload["commission_first_year_pct"],
                        payload["commission_recurring_pct"],
                        payload.get("notes"),
                    ),
                ).fetchone()
                current = dict(row)
                _audit(conn, claims, "contract.create", "contract", str(current["id"]), None, current, ip)
            else:
                row = conn.execute(
                    """
                    UPDATE billing.contracts
                    SET
                      tenant_id = %s,
                      channel_id = %s,
                      plan_name = %s,
                      monthly_amount = %s,
                      billing_day = %s,
                      issue_day = %s,
                      start_date = %s,
                      end_date = %s,
                      is_enabled = %s,
                      commission_first_year_pct = %s,
                      commission_recurring_pct = %s,
                      notes = %s,
                      updated_at = now()
                    WHERE id = %s
                    RETURNING *
                    """,
                    (
                        payload["tenant_id"],
                        payload.get("channel_id"),
                        payload["plan_name"],
                        payload["monthly_amount"],
                        payload["billing_day"],
                        payload["issue_day"],
                        payload["start_date"],
                        payload.get("end_date"),
                        bool(payload.get("is_enabled", True)),
                        payload["commission_first_year_pct"],
                        payload["commission_recurring_pct"],
                        payload.get("notes"),
                        contract_id,
                    ),
                ).fetchone()
                current = dict(row)
                _audit(conn, claims, "contract.update", "contract", str(current["id"]), previous, current, ip)

        _sync_active_contract_summary(conn, int(current["tenant_id"]))
        if previous is None:
            _audit(conn, claims, "contract.create", "contract", str(current["id"]), None, current, ip)
        conn.commit()
    return current


def _contract_generates_for_competence(contract: dict[str, Any], competence_month: date) -> bool:
    competence_month = _month_start(competence_month)
    start_month = _month_start(contract["start_date"])
    if competence_month < start_month:
        return False
    end_date = contract.get("end_date")
    if end_date and competence_month > _month_start(end_date):
        return False
    return bool(contract.get("is_enabled", True))


def _receivable_status(as_of: date, issue_date: date, due_date: date, is_emitted: bool, paid_at: Any, cancelled: bool) -> str:
    if cancelled:
        return "cancelled"
    if paid_at:
        return "paid"
    if due_date < as_of:
        return "overdue"
    if is_emitted:
        return "issued"
    if issue_date <= as_of:
        return "open"
    return "planned"


def generate_receivables(
    claims: dict[str, Any],
    ip: str | None,
    competence_month: date | None = None,
    as_of: date | None = None,
    months_ahead: int = 0,
    tenant_id: int | None = None,
) -> dict[str, Any]:
    _require_platform_master(claims)
    as_of = as_of or date.today()
    start_competence = _month_start(competence_month or as_of)
    created: list[dict[str, Any]] = []
    updated_statuses = 0

    with _connect() as conn:
        params: list[Any] = []
        tenant_filter = ""
        if tenant_id is not None:
            _assert_company_visible(claims, tenant_id)
            tenant_filter = " AND c.tenant_id = %s "
            params.append(tenant_id)
        contracts = conn.execute(
            f"""
            SELECT c.*, t.status AS tenant_status
            FROM billing.contracts c
            JOIN app.tenants t
              ON t.id_empresa = c.tenant_id
            WHERE c.is_enabled = true
              AND t.status <> 'cancelled'
              {tenant_filter}
            ORDER BY c.tenant_id, c.start_date DESC
            """,
            params,
        ).fetchall()

        for contract in contracts:
            for month_offset in range(months_ahead + 1):
                competence = _add_months(start_competence, month_offset)
                if not _contract_generates_for_competence(dict(contract), competence):
                    continue
                issue_date = _safe_month_date(competence.year, competence.month, int(contract["issue_day"]))
                due_date = _safe_month_date(competence.year, competence.month, int(contract["billing_day"]))
                status = _receivable_status(
                    as_of=as_of,
                    issue_date=issue_date,
                    due_date=due_date,
                    is_emitted=False,
                    paid_at=None,
                    cancelled=False,
                )
                row = conn.execute(
                    """
                    INSERT INTO billing.receivables (
                      tenant_id,
                      contract_id,
                      competence_month,
                      issue_date,
                      due_date,
                      amount,
                      status,
                      is_emitted
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, false)
                    ON CONFLICT (tenant_id, contract_id, competence_month)
                    DO UPDATE SET
                      issue_date = EXCLUDED.issue_date,
                      due_date = EXCLUDED.due_date,
                      amount = EXCLUDED.amount,
                      status = CASE
                        WHEN billing.receivables.status IN ('paid', 'cancelled') THEN billing.receivables.status
                        ELSE EXCLUDED.status
                      END,
                      updated_at = now()
                    RETURNING *
                    """,
                    (
                        contract["tenant_id"],
                        contract["id"],
                        competence,
                        issue_date,
                        due_date,
                        contract["monthly_amount"],
                        status,
                    ),
                ).fetchone()
                receivable = dict(row)
                if receivable["created_at"] == receivable["updated_at"]:
                    created.append(receivable)
                    _audit(conn, claims, "receivable.generate", "receivable", str(receivable["id"]), None, receivable, ip)

        updated_statuses = refresh_receivable_statuses(conn, as_of=as_of, claims=claims, ip=ip)
        conn.commit()

    return {"created": len(created), "status_updates": updated_statuses, "items": created}


def refresh_receivable_statuses(conn, as_of: date, claims: dict[str, Any] | None = None, ip: str | None = None) -> int:
    rows = conn.execute(
        """
        SELECT *
        FROM billing.receivables
        WHERE status NOT IN ('paid', 'cancelled')
        """
    ).fetchall()
    changed = 0
    for row in rows:
        current = dict(row)
        new_status = _receivable_status(
            as_of=as_of,
            issue_date=current["issue_date"],
            due_date=current["due_date"],
            is_emitted=bool(current.get("is_emitted")),
            paid_at=current.get("paid_at"),
            cancelled=False,
        )
        if new_status != current["status"]:
            conn.execute(
                "UPDATE billing.receivables SET status = %s, updated_at = now() WHERE id = %s",
                (new_status, current["id"]),
            )
            changed += 1
            if claims:
                updated = {**current, "status": new_status}
                _audit(conn, claims, "receivable.status.refresh", "receivable", str(current["id"]), current, updated, ip)
    return changed


def list_receivables(
    claims: dict[str, Any],
    tenant_id: int | None = None,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    _require_platform_master(claims)
    params: list[Any] = []
    filters = ""
    if tenant_id is not None:
        filters += " AND r.tenant_id = %s "
        params.append(tenant_id)
    if status:
        filters += " AND r.status = %s "
        params.append(status)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
              r.*,
              t.nome AS tenant_name,
              t.channel_id,
              ch.name AS channel_name,
              c.plan_name
            FROM billing.receivables r
            JOIN app.tenants t
              ON t.id_empresa = r.tenant_id
            LEFT JOIN billing.contracts c
              ON c.id = r.contract_id
            LEFT JOIN app.channels ch
              ON ch.id = t.channel_id
            WHERE 1 = 1
            {filters}
            ORDER BY r.competence_month DESC, r.id DESC
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        ).fetchall()
        total = conn.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM billing.receivables r
            WHERE 1 = 1
            {filters}
            """,
            params,
        ).fetchone()
    return {"items": [dict(row) for row in rows], "total": int(total["total"] or 0)}


def _commission_pct_for_contract(contract: dict[str, Any], competence_month: date) -> Decimal:
    start = contract["start_date"]
    first_year_cutoff = date(start.year + 1, start.month, min(start.day, 28)) if start.month == 2 and start.day > 28 else start.replace(year=start.year + 1)
    if competence_month < _month_start(first_year_cutoff):
        return Decimal(contract["commission_first_year_pct"])
    return Decimal(contract["commission_recurring_pct"])


def _generate_channel_payable(conn, claims: dict[str, Any], receivable: dict[str, Any], ip: str | None) -> dict[str, Any] | None:
    contract = conn.execute("SELECT * FROM billing.contracts WHERE id = %s", (receivable["contract_id"],)).fetchone()
    if not contract:
        return None
    contract_dict = dict(contract)
    channel_id = contract_dict.get("channel_id")
    if not channel_id:
        return None
    pct = _commission_pct_for_contract(contract_dict, receivable["competence_month"])
    payable_amount = (Decimal(receivable["received_amount"] or receivable["amount"]) * pct) / Decimal("100")
    previous = conn.execute(
        "SELECT * FROM billing.channel_payables WHERE receivable_id = %s",
        (receivable["id"],),
    ).fetchone()
    previous_dict = dict(previous) if previous else None
    row = conn.execute(
        """
        INSERT INTO billing.channel_payables (
          tenant_id,
          channel_id,
          receivable_id,
          competence_month,
          commission_pct,
          gross_amount,
          payable_amount,
          status,
          due_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'released', NULL)
        ON CONFLICT (receivable_id)
        DO UPDATE SET
          commission_pct = EXCLUDED.commission_pct,
          gross_amount = EXCLUDED.gross_amount,
          payable_amount = EXCLUDED.payable_amount,
          status = CASE
            WHEN billing.channel_payables.status = 'paid' THEN billing.channel_payables.status
            ELSE EXCLUDED.status
          END,
          updated_at = now()
        RETURNING *
        """,
        (
            receivable["tenant_id"],
            channel_id,
            receivable["id"],
            receivable["competence_month"],
            pct,
            receivable["received_amount"] or receivable["amount"],
            payable_amount.quantize(Decimal("0.01")),
        ),
    ).fetchone()
    payable = dict(row)
    if previous_dict != payable:
        _audit(
            conn,
            claims,
            "channel_payable.generate" if previous_dict is None else "channel_payable.refresh",
            "channel_payable",
            str(payable["id"]),
            previous_dict,
            payable,
            ip,
        )
    return payable


def _recalculate_receivable_status(current: dict[str, Any], as_of: date | None = None) -> str:
    return _receivable_status(
        as_of=as_of or _utcnow().date(),
        issue_date=current["issue_date"],
        due_date=current["due_date"],
        is_emitted=bool(current.get("is_emitted")),
        paid_at=current.get("paid_at"),
        cancelled=str(current.get("status")) == "cancelled",
    )


def _get_channel_payable_for_receivable(conn, receivable_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM billing.channel_payables WHERE receivable_id = %s",
        (receivable_id,),
    ).fetchone()
    return dict(row) if row else None


def mark_receivable_emitted(claims: dict[str, Any], receivable_id: int, payload: dict[str, Any], ip: str | None) -> dict[str, Any]:
    _require_platform_master(claims)
    emitted_at = payload.get("emitted_at") or _utcnow()
    with _connect() as conn:
        previous = conn.execute("SELECT * FROM billing.receivables WHERE id = %s", (receivable_id,)).fetchone()
        if not previous:
            raise AuthError(404, "receivable_not_found", "Conta a receber não encontrada.")
        previous = dict(previous)
        if previous["status"] == "cancelled":
            raise AuthError(409, "receivable_cancelled", "Conta cancelada não pode ser emitida.")
        conn.execute(
            """
            UPDATE billing.receivables
            SET
              is_emitted = true,
              emitted_at = %s,
              status = CASE
                WHEN status IN ('paid', 'cancelled') THEN status
                WHEN due_date < CURRENT_DATE THEN 'overdue'
                ELSE 'issued'
              END,
              notes = COALESCE(%s, notes),
              updated_at = now()
            WHERE id = %s
            """,
            (emitted_at, payload.get("notes"), receivable_id),
        )
        current = conn.execute("SELECT * FROM billing.receivables WHERE id = %s", (receivable_id,)).fetchone()
        current_dict = dict(current)
        _audit(conn, claims, "receivable.mark_emitted", "receivable", str(receivable_id), previous, current_dict, ip)
        conn.commit()
    return current_dict


def unmark_receivable_emitted(claims: dict[str, Any], receivable_id: int, notes: str | None, ip: str | None) -> dict[str, Any]:
    _require_platform_master(claims)
    with _connect() as conn:
        previous = conn.execute("SELECT * FROM billing.receivables WHERE id = %s", (receivable_id,)).fetchone()
        if not previous:
            raise AuthError(404, "receivable_not_found", "Conta a receber não encontrada.")
        previous = dict(previous)
        if previous["status"] == "paid":
            raise AuthError(409, "receivable_paid", "Conta paga não pode desfazer emissão.")
        if previous["status"] == "cancelled":
            raise AuthError(409, "receivable_cancelled", "Conta cancelada não pode desfazer emissão.")
        conn.execute(
            """
            UPDATE billing.receivables
            SET
              is_emitted = false,
              emitted_at = NULL,
              notes = COALESCE(%s, notes),
              updated_at = now()
            WHERE id = %s
            """,
            (notes, receivable_id),
        )
        current = conn.execute("SELECT * FROM billing.receivables WHERE id = %s", (receivable_id,)).fetchone()
        current_dict = dict(current)
        recalculated = _recalculate_receivable_status(current_dict)
        conn.execute(
            "UPDATE billing.receivables SET status = %s, updated_at = now() WHERE id = %s",
            (recalculated, receivable_id),
        )
        current_dict["status"] = recalculated
        _audit(conn, claims, "receivable.unmark_emitted", "receivable", str(receivable_id), previous, current_dict, ip)
        conn.commit()
    return current_dict


def mark_receivable_paid(claims: dict[str, Any], receivable_id: int, payload: dict[str, Any], ip: str | None) -> dict[str, Any]:
    _require_platform_master(claims)
    paid_at = payload.get("paid_at") or _utcnow()
    with _connect() as conn:
        previous = conn.execute("SELECT * FROM billing.receivables WHERE id = %s", (receivable_id,)).fetchone()
        if not previous:
            raise AuthError(404, "receivable_not_found", "Conta a receber não encontrada.")
        previous = dict(previous)
        if previous["status"] == "cancelled":
            raise AuthError(409, "receivable_cancelled", "Conta cancelada não pode ser paga.")

        received_amount = payload.get("received_amount") or previous["amount"]
        if previous["status"] == "paid":
            same_amount = Decimal(previous.get("received_amount") or previous["amount"]) == Decimal(received_amount)
            same_method = (previous.get("payment_method") or None) == (payload.get("payment_method") or None)
            existing_payable = _get_channel_payable_for_receivable(conn, receivable_id)
            if same_amount and same_method:
                return {"receivable": previous, "channel_payable": existing_payable}
            raise AuthError(409, "receivable_already_paid", "Conta já está paga. Desfaça o pagamento antes de alterar.")
        conn.execute(
            """
            UPDATE billing.receivables
            SET
              paid_at = %s,
              received_amount = %s,
              payment_method = %s,
              is_emitted = true,
              emitted_at = COALESCE(emitted_at, %s),
              notes = COALESCE(%s, notes),
              status = 'paid',
              updated_at = now()
            WHERE id = %s
            """,
            (paid_at, received_amount, payload.get("payment_method"), paid_at, payload.get("notes"), receivable_id),
        )
        current = conn.execute("SELECT * FROM billing.receivables WHERE id = %s", (receivable_id,)).fetchone()
        current_dict = dict(current)
        payable = _generate_channel_payable(conn, claims, current_dict, ip)
        _audit(conn, claims, "receivable.mark_paid", "receivable", str(receivable_id), previous, current_dict, ip)
        conn.commit()
    return {"receivable": current_dict, "channel_payable": payable}


def undo_receivable_payment(claims: dict[str, Any], receivable_id: int, notes: str | None, ip: str | None) -> dict[str, Any]:
    _require_platform_master(claims)
    with _connect() as conn:
        previous = conn.execute("SELECT * FROM billing.receivables WHERE id = %s", (receivable_id,)).fetchone()
        if not previous:
            raise AuthError(404, "receivable_not_found", "Conta a receber não encontrada.")
        previous = dict(previous)
        if previous["status"] != "paid":
            raise AuthError(409, "receivable_not_paid", "Conta não está paga.")

        payable = _get_channel_payable_for_receivable(conn, receivable_id)
        if payable and payable["status"] == "paid":
            raise AuthError(409, "channel_payable_paid", "Conta de canal já paga. Desfaça o payable antes do recebimento.")
        if payable and payable["status"] != "cancelled":
            payable_next = {**payable, "status": "cancelled", "notes": notes or payable.get("notes")}
            conn.execute(
                """
                UPDATE billing.channel_payables
                SET
                  status = 'cancelled',
                  notes = COALESCE(%s, notes),
                  updated_at = now()
                WHERE id = %s
                """,
                (notes, payable["id"]),
            )
            _audit(
                conn,
                claims,
                "channel_payable.cancel",
                "channel_payable",
                str(payable["id"]),
                payable,
                payable_next,
                ip,
            )

        conn.execute(
            """
            UPDATE billing.receivables
            SET
              paid_at = NULL,
              received_amount = NULL,
              payment_method = NULL,
              notes = COALESCE(%s, notes),
              updated_at = now()
            WHERE id = %s
            """,
            (notes, receivable_id),
        )
        current = conn.execute("SELECT * FROM billing.receivables WHERE id = %s", (receivable_id,)).fetchone()
        current_dict = dict(current)
        recalculated = _recalculate_receivable_status(current_dict)
        conn.execute(
            "UPDATE billing.receivables SET status = %s, updated_at = now() WHERE id = %s",
            (recalculated, receivable_id),
        )
        current_dict["status"] = recalculated
        _audit(conn, claims, "receivable.undo_paid", "receivable", str(receivable_id), previous, current_dict, ip)
        payable_after = _get_channel_payable_for_receivable(conn, receivable_id)
        conn.commit()
    return {"receivable": current_dict, "channel_payable": payable_after}


def cancel_receivable(claims: dict[str, Any], receivable_id: int, notes: str | None, ip: str | None) -> dict[str, Any]:
    _require_platform_master(claims)
    with _connect() as conn:
        previous = conn.execute("SELECT * FROM billing.receivables WHERE id = %s", (receivable_id,)).fetchone()
        if not previous:
            raise AuthError(404, "receivable_not_found", "Conta a receber não encontrada.")
        previous = dict(previous)
        if previous["status"] == "paid":
            raise AuthError(409, "receivable_paid", "Conta paga não pode ser cancelada.")
        if previous["status"] == "cancelled":
            return previous
        conn.execute(
            """
            UPDATE billing.receivables
            SET
              status = 'cancelled',
              notes = COALESCE(%s, notes),
              updated_at = now()
            WHERE id = %s
            """,
            (notes, receivable_id),
        )
        current = conn.execute("SELECT * FROM billing.receivables WHERE id = %s", (receivable_id,)).fetchone()
        current_dict = dict(current)
        _audit(conn, claims, "receivable.cancel", "receivable", str(receivable_id), previous, current_dict, ip)
        conn.commit()
    return current_dict


def reopen_receivable(claims: dict[str, Any], receivable_id: int, notes: str | None, ip: str | None) -> dict[str, Any]:
    _require_platform_master(claims)
    with _connect() as conn:
        previous = conn.execute("SELECT * FROM billing.receivables WHERE id = %s", (receivable_id,)).fetchone()
        if not previous:
            raise AuthError(404, "receivable_not_found", "Conta a receber não encontrada.")
        previous = dict(previous)
        if previous["status"] != "cancelled":
            raise AuthError(409, "receivable_not_cancelled", "Somente contas canceladas podem ser reabertas.")
        conn.execute(
            """
            UPDATE billing.receivables
            SET
              notes = COALESCE(%s, notes),
              updated_at = now()
            WHERE id = %s
            """,
            (notes, receivable_id),
        )
        current = conn.execute("SELECT * FROM billing.receivables WHERE id = %s", (receivable_id,)).fetchone()
        current_dict = dict(current)
        current_dict["status"] = _receivable_status(
            as_of=_utcnow().date(),
            issue_date=current_dict["issue_date"],
            due_date=current_dict["due_date"],
            is_emitted=bool(current_dict.get("is_emitted")),
            paid_at=None,
            cancelled=False,
        )
        conn.execute(
            "UPDATE billing.receivables SET status = %s, updated_at = now() WHERE id = %s",
            (current_dict["status"], receivable_id),
        )
        _audit(conn, claims, "receivable.reopen", "receivable", str(receivable_id), previous, current_dict, ip)
        conn.commit()
    return current_dict


def list_channel_payables(
    claims: dict[str, Any],
    channel_id: int | None = None,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    _require_platform_master(claims)
    params: list[Any] = []
    filters = ""
    if channel_id is not None:
        filters += " AND p.channel_id = %s "
        params.append(channel_id)
    if status:
        filters += " AND p.status = %s "
        params.append(status)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
              p.*,
              ch.name AS channel_name,
              t.nome AS tenant_name
            FROM billing.channel_payables p
            JOIN app.channels ch
              ON ch.id = p.channel_id
            JOIN app.tenants t
              ON t.id_empresa = p.tenant_id
            WHERE 1 = 1
            {filters}
            ORDER BY p.competence_month DESC, p.id DESC
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        ).fetchall()
        total = conn.execute(
            f"SELECT COUNT(*) AS total FROM billing.channel_payables p WHERE 1 = 1 {filters}",
            params,
        ).fetchone()
    return {"items": [dict(row) for row in rows], "total": int(total["total"] or 0)}


def mark_channel_payable_paid(claims: dict[str, Any], payable_id: int, payload: dict[str, Any], ip: str | None) -> dict[str, Any]:
    _require_platform_master(claims)
    paid_at = payload.get("paid_at") or _utcnow()
    with _connect() as conn:
        previous = conn.execute("SELECT * FROM billing.channel_payables WHERE id = %s", (payable_id,)).fetchone()
        if not previous:
            raise AuthError(404, "channel_payable_not_found", "Conta a pagar não encontrada.")
        previous = dict(previous)
        if previous["status"] == "cancelled":
            raise AuthError(409, "channel_payable_cancelled", "Conta cancelada não pode ser paga.")
        if previous["status"] == "paid":
            return previous
        conn.execute(
            """
            UPDATE billing.channel_payables
            SET
              status = 'paid',
              paid_at = %s,
              notes = COALESCE(%s, notes),
              updated_at = now()
            WHERE id = %s
            """,
            (paid_at, payload.get("notes"), payable_id),
        )
        current = conn.execute("SELECT * FROM billing.channel_payables WHERE id = %s", (payable_id,)).fetchone()
        current_dict = dict(current)
        _audit(conn, claims, "channel_payable.mark_paid", "channel_payable", str(payable_id), previous, current_dict, ip)
        conn.commit()
    return current_dict


def cancel_channel_payable(claims: dict[str, Any], payable_id: int, notes: str | None, ip: str | None) -> dict[str, Any]:
    _require_platform_master(claims)
    with _connect() as conn:
        previous = conn.execute("SELECT * FROM billing.channel_payables WHERE id = %s", (payable_id,)).fetchone()
        if not previous:
            raise AuthError(404, "channel_payable_not_found", "Conta a pagar não encontrada.")
        previous = dict(previous)
        if previous["status"] == "paid":
            raise AuthError(409, "channel_payable_paid", "Conta paga não pode ser cancelada.")
        if previous["status"] == "cancelled":
            return previous
        conn.execute(
            """
            UPDATE billing.channel_payables
            SET
              status = 'cancelled',
              notes = COALESCE(%s, notes),
              updated_at = now()
            WHERE id = %s
            """,
            (notes, payable_id),
        )
        current = conn.execute("SELECT * FROM billing.channel_payables WHERE id = %s", (payable_id,)).fetchone()
        current_dict = dict(current)
        _audit(conn, claims, "channel_payable.cancel", "channel_payable", str(payable_id), previous, current_dict, ip)
        conn.commit()
    return current_dict


def list_notification_subscriptions(
    claims: dict[str, Any],
    tenant_id: int | None = None,
    user_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    _require_platform_operations(claims)
    role = normalize_role(claims.get("user_role"))
    params: list[Any] = []
    filters = ""
    if tenant_id is not None:
        _assert_company_visible(claims, tenant_id)
        filters += " AND s.tenant_id = %s "
        params.append(tenant_id)
    elif role == "channel_admin":
        visible_channels = list(claims.get("channel_ids") or [])
        if not visible_channels:
            return {"items": [], "total": 0}
        filters += " AND EXISTS (SELECT 1 FROM app.tenants t WHERE t.id_empresa = s.tenant_id AND t.channel_id = ANY(%s)) "
        params.append(visible_channels)
    if user_id is not None:
        filters += " AND s.user_id = %s::uuid "
        params.append(user_id)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
              s.*,
              u.nome AS user_name,
              u.email AS user_email,
              t.nome AS tenant_name,
              f.nome AS branch_name
            FROM app.notification_subscriptions s
            JOIN auth.users u
              ON u.id = s.user_id
            LEFT JOIN app.tenants t
              ON t.id_empresa = s.tenant_id
            LEFT JOIN auth.filiais f
              ON f.id_empresa = s.tenant_id
             AND f.id_filial = s.branch_id
            WHERE 1 = 1
            {filters}
            ORDER BY s.created_at DESC, s.id DESC
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        ).fetchall()
        total = conn.execute(
            f"SELECT COUNT(*) AS total FROM app.notification_subscriptions s WHERE 1 = 1 {filters}",
            params,
        ).fetchone()
    return {"items": [dict(row) for row in rows], "total": int(total["total"] or 0)}


def upsert_notification_subscription(
    claims: dict[str, Any],
    payload: dict[str, Any],
    ip: str | None,
    subscription_id: int | None = None,
) -> dict[str, Any]:
    _require_platform_operations(claims)
    if not any(item["id"] == payload["user_id"] for item in list_users(claims, limit=5000, offset=0)["items"]):
        raise AuthError(403, "user_access_denied", "Acesso não permitido ao usuário.")
    if payload.get("tenant_id") is not None:
        _assert_company_visible(claims, int(payload["tenant_id"]))
    with _connect() as conn:
        previous = None
        if subscription_id is None:
            row = conn.execute(
                """
                INSERT INTO app.notification_subscriptions (
                  user_id,
                  tenant_id,
                  branch_id,
                  event_type,
                  channel,
                  severity_min,
                  is_enabled
                )
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    payload["user_id"],
                    payload.get("tenant_id"),
                    payload.get("branch_id"),
                    payload["event_type"],
                    payload["channel"],
                    payload.get("severity_min"),
                    bool(payload.get("is_enabled", True)),
                ),
            ).fetchone()
        else:
            previous = conn.execute("SELECT * FROM app.notification_subscriptions WHERE id = %s", (subscription_id,)).fetchone()
            if not previous:
                raise AuthError(404, "subscription_not_found", "Assinatura não encontrada.")
            previous = dict(previous)
            row = conn.execute(
                """
                UPDATE app.notification_subscriptions
                SET
                  user_id = %s::uuid,
                  tenant_id = %s,
                  branch_id = %s,
                  event_type = %s,
                  channel = %s,
                  severity_min = %s,
                  is_enabled = %s,
                  updated_at = now()
                WHERE id = %s
                RETURNING *
                """,
                (
                    payload["user_id"],
                    payload.get("tenant_id"),
                    payload.get("branch_id"),
                    payload["event_type"],
                    payload["channel"],
                    payload.get("severity_min"),
                    bool(payload.get("is_enabled", True)),
                    subscription_id,
                ),
            ).fetchone()
        current = dict(row)
        _audit(conn, claims, "notification_subscription.update" if previous else "notification_subscription.create", "notification_subscription", str(current["id"]), previous, current, ip)
        conn.commit()
    return current


def list_audit(
    claims: dict[str, Any],
    tenant_id: int | None = None,
    entity_type: str | None = None,
    action: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    entity_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    _require_platform_master(claims)
    params: list[Any] = []
    filters = ""
    if tenant_id is not None:
        filters = """
          AND (
            (entity_type = 'tenant' AND entity_id = %s)
            OR (entity_type = 'branch' AND entity_id LIKE %s)
            OR (entity_type = 'contract' AND (new_values->>'tenant_id' = %s OR old_values->>'tenant_id' = %s))
            OR (entity_type = 'receivable' AND (new_values->>'tenant_id' = %s OR old_values->>'tenant_id' = %s))
            OR (entity_type = 'channel_payable' AND (new_values->>'tenant_id' = %s OR old_values->>'tenant_id' = %s))
          )
        """
        params = [
            str(tenant_id),
            f"{tenant_id}:%",
            str(tenant_id),
            str(tenant_id),
            str(tenant_id),
            str(tenant_id),
            str(tenant_id),
            str(tenant_id),
        ]
    else:
        params = []

    if entity_type:
        filters += " AND entity_type = %s "
        params.append(entity_type)
    if action:
        filters += " AND action = %s "
        params.append(action)
    if entity_id:
        filters += " AND entity_id = %s "
        params.append(entity_id)
    if date_from:
        filters += " AND created_at >= %s::date "
        params.append(date_from)
    if date_to:
        filters += " AND created_at < (%s::date + interval '1 day') "
        params.append(date_to)
    params.append(limit)

    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM audit.audit_log
            WHERE 1 = 1
            {filters}
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]
