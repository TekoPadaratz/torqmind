from __future__ import annotations
from typing import Optional, Tuple, Dict, Any, List
from app.db import get_conn
from app.security import verify_password

def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        return conn.execute("SELECT id, email, password_hash, is_active FROM auth.users WHERE email=%s", (email,)).fetchone()

def get_user_scopes(user_id: str) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute("SELECT role, id_empresa, id_filial FROM auth.user_tenants WHERE user_id=%s", (user_id,)).fetchall()
        return list(rows)

def verify_login(email: str, password: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    user = get_user_by_email(email)
    if not user or not user["is_active"]:
        raise ValueError("Invalid credentials")
    if not verify_password(password, user["password_hash"]):
        raise ValueError("Invalid credentials")
    scopes = get_user_scopes(str(user["id"]))
    if not scopes:
        raise ValueError("User has no scope")
    scopes_sorted = sorted(scopes, key=lambda s: {"MASTER":0,"OWNER":1,"MANAGER":2}.get(s["role"], 9))
    return user, scopes_sorted[0]
