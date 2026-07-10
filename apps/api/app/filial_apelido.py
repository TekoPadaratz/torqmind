"""Resolução do apelido curto de filial (definido na Plataforma).

O apelido curto (ex.: "VR 01") sobrepõe o nome completo do cadastro em TODAS as
telas, rankings e alertas. É resolvido por um mapa por empresa, carregado do
Postgres (``auth.filiais.apelido``) com cache TTL curto e exposto via um
``ContextVar`` semeado uma única vez por requisição em ``scope.resolve_scope``.

Fluxo:
- ``set_apelido_scope(id_empresa)``  -> chamado no resolve_scope; fixa a empresa
  da requisição e pré-aquece o cache (evita I/O durante a iteração de linhas).
- ``apelido_for(id_filial)``          -> usado por ``_filial_label``; retorna o
  apelido da filial (ou ``None`` para cair no nome completo).
- ``invalidate_apelido_cache(...)``   -> chamado ao editar a filial na Plataforma.
"""

from __future__ import annotations

import threading
import time
from contextvars import ContextVar
from typing import Dict, Optional

from app.db import get_conn

# TTL curto: mudanças de apelido na Plataforma refletem em <=60s mesmo sem
# invalidação explícita (que também existe no upsert da filial).
_TTL_SECONDS = 60.0

_lock = threading.Lock()
_cache: Dict[int, tuple[float, Dict[int, str]]] = {}
_current_empresa: ContextVar[Optional[int]] = ContextVar(
    "_apelido_current_empresa", default=None
)


def _load_from_db(id_empresa: int) -> Dict[int, str]:
    try:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            rows = conn.execute(
                """
                SELECT id_filial, apelido
                FROM auth.filiais
                WHERE id_empresa = %s
                  AND apelido IS NOT NULL
                  AND btrim(apelido) <> ''
                """,
                (id_empresa,),
            ).fetchall()
        return {
            int(row["id_filial"]): str(row["apelido"]).strip()
            for row in rows
            if row.get("id_filial") is not None
        }
    except Exception:
        # Nunca deixar a resolução do apelido derrubar uma tela: sem mapa,
        # cai no nome completo (comportamento anterior).
        return {}


def load_apelido_map(id_empresa: Optional[int]) -> Dict[int, str]:
    if id_empresa is None:
        return {}
    empresa = int(id_empresa)
    now = time.monotonic()
    with _lock:
        cached = _cache.get(empresa)
        if cached and (now - cached[0]) < _TTL_SECONDS:
            return cached[1]
    fresh = _load_from_db(empresa)
    with _lock:
        _cache[empresa] = (now, fresh)
    return fresh


def set_apelido_scope(id_empresa: Optional[int]) -> None:
    """Fixa a empresa da requisição e pré-aquece o cache do apelido."""
    _current_empresa.set(int(id_empresa) if id_empresa is not None else None)
    if id_empresa is not None:
        try:
            load_apelido_map(int(id_empresa))
        except Exception:
            pass


def apelido_for(id_filial) -> Optional[str]:
    empresa = _current_empresa.get()
    if empresa is None or id_filial is None:
        return None
    try:
        fid = int(id_filial)
    except (TypeError, ValueError):
        return None
    apelido = load_apelido_map(empresa).get(fid)
    return apelido or None


def invalidate_apelido_cache(id_empresa: Optional[int] = None) -> None:
    with _lock:
        if id_empresa is None:
            _cache.clear()
        else:
            _cache.pop(int(id_empresa), None)
