"""Hom demo identity mask — presentation-layer PII redaction (LGPD / demo).

Enabled only when ``DEMO_IDENTITY_MASK=true``. Production **must** refuse boot
with the flag on (see ``runtime_guard``). Does not touch ClickHouse/PostgreSQL —
walks JSON payloads already built for the response, same cost class as
``redact_sensitive``.

Stable id→label maps live in-process so rankings/charts do not jitter across
requests within the same API worker.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Optional

# Bump when mask semantics change (included in BI snapshot cache context).
IDENTITY_MASK_VERSION = 2

# ── Allowlist of identity field keys (matched case-insensitively) ──────────

_COMPANY_KEYS = frozenset(
    {
        "empresa",
        "nome_empresa",
        "empresa_nome",
        "company_name",
        "tenant_name",
        "razao_social",
        "nome_fantasia",
        "fantasia",
    }
)

_FILIAL_KEYS = frozenset(
    {
        "filial_label",
        "filial_nome",
        "nome_filial",
        "branch_name",
        "apelido",
        "nome_completo",  # list_filiais companion of apelido/nome
        "nome_resumido",  # ANP / grids curtos de filial
    }
)

_PERSON_KEYS = frozenset(
    {
        "nome_vendedor",
        "nome_funcionario",
        "nome_funcionario_snapshot",
        "funcionario_nome",
        "vendedor_nome",
        "operador_label",
        "operador_nome",
        "nome_operador",
        "usuario_label",
        "usuario_nome",
        "nome_usuario",
        "user_name",
        "user_label",
        "frentista",
        "nome_frentista",
        "responsavel",
        "nome_responsavel",
        "vendedor",
        "funcionario",
        "operador",
        "colaborador",
        "nome_colaborador",
        "nome_gerente",
        "gerente_nome",
        "gerente",
    }
)

# Free-text fields that often embed filial nicknames / bank account PII
_FREE_TEXT_KEYS = frozenset(
    {
        "label",
        "descricao",
        "descricao_conta",
        "nome_conta",
        "conta",
        "conta_label",
        "historico",
        "titulo",
        "title",
    }
)

# Bank / filial nickname leaks inside free text (e.g. "CONTA VR07 AG:…")
_ACCOUNTISH_RE = re.compile(
    r"(?i)(\bconta\b|\bag\s*:|\bc/?c\s*:|\bvr\s*\d+\b|@|cnpj|cpf)"
)
_FILIAL_NICK_RE = re.compile(r"(?i)^\s*vr\s*\d+\s*$")

_CLIENT_KEYS = frozenset(
    {
        "cliente",
        "cliente_nome",
        "nome_cliente",
        "entidade_nome",
        "nome_entidade",
        "entidade",
        "entidade_de",
        "entidade_para",
        "cliente_label",
    }
)

_SUPPLIER_KEYS = frozenset(
    {
        "fornecedor",
        "fornecedor_nome",
        "nome_fornecedor",
        "supplier",
        "supplier_name",
    }
)

_CONTACT_KEYS = frozenset(
    {
        "cpf",
        "cnpj",
        "cpf_cnpj",
        "documento_cliente",
        "doc_cliente",
        "telefone",
        "celular",
        "fone",
        "phone",
        "email",
        "e_mail",
        "mail",
    }
)

# Session / auth display fields (bare "nome" is handled contextually below)
_SESSION_PERSON_KEYS = frozenset({"name", "username"})

# Keys that look like names but must stay (products, payments, categories, etc.)
_NEVER_MASK_KEYS = frozenset(
    {
        "documento",
        "nfe",
        "numero_nfe",
        "nro_nfe",
        "nfc",
        "chave_nfe",
        "forma_pagamento",
        "forma_pgto",
        "pagamento",
        "produto",
        "produto_nome",
        "nome_produto",
        "combustivel",
        "grupo",
        "grupo_nome",
        "categoria",
        "categoria_nome",
        "role_label",
        "user_role",
        "analytics_role",
        "data_state",
        "message",
        "messages",
        "error",
        "detail",
    }
)

_MASKED_EMAIL = "***@***"
_MASKED_CPF = "***.***.***-**"
_MASKED_CNPJ = "**.***.***/****-**"
_MASKED_PHONE = "(**) *****-****"

# Process-local ordinal maps (stable per worker lifetime).
_filial_ordinal: dict[int, int] = {}
_empresa_ordinal: dict[int, int] = {}
_next_filial = 1
_next_empresa = 1


def identity_mask_enabled() -> bool:
    try:
        from app.config import settings

        return bool(getattr(settings, "demo_identity_mask", False))
    except Exception:
        return False


def identity_mask_cache_version() -> int:
    """Value for BI snapshot ``mask_v`` — 0 when off, IDENTITY_MASK_VERSION when on."""
    return IDENTITY_MASK_VERSION if identity_mask_enabled() else 0


def reset_identity_maps_for_tests() -> None:
    """Clear ordinal maps — tests only."""
    global _next_filial, _next_empresa
    _filial_ordinal.clear()
    _empresa_ordinal.clear()
    _next_filial = 1
    _next_empresa = 1


def _stable_int_from_text(text: str) -> int:
    digest = hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()
    return int(digest[:8], 16) % 100_000


def label_empresa(id_empresa: Optional[int], original: Optional[str] = None) -> str:
    global _next_empresa
    if id_empresa is not None:
        eid = int(id_empresa)
        if eid not in _empresa_ordinal:
            _empresa_ordinal[eid] = _next_empresa
            _next_empresa += 1
        return f"Empresa {_empresa_ordinal[eid]}"
    if original:
        return f"Empresa {_stable_int_from_text(original)}"
    return "Empresa"


def label_filial(id_filial: Optional[int], original: Optional[str] = None) -> str:
    global _next_filial
    if id_filial is not None:
        fid = int(id_filial)
        if fid not in _filial_ordinal:
            _filial_ordinal[fid] = _next_filial
            _next_filial += 1
        return f"Filial {_filial_ordinal[fid]}"
    if original:
        return f"Filial {_stable_int_from_text(original)}"
    return "Filial"


def label_pessoa(person_id: Optional[int], original: Optional[str] = None) -> str:
    if person_id is not None:
        return f"Colaborador {int(person_id)}"
    if original:
        return f"Colaborador {_stable_int_from_text(original)}"
    return "Colaborador"


def label_cliente(client_id: Optional[int], original: Optional[str] = None) -> str:
    if client_id is not None:
        return f"Cliente {int(client_id)}"
    if original:
        return f"Cliente {_stable_int_from_text(original)}"
    return "Cliente"


def label_fornecedor(entity_id: Optional[int], original: Optional[str] = None) -> str:
    if entity_id is not None:
        return f"Fornecedor {int(entity_id)}"
    if original:
        return f"Fornecedor {_stable_int_from_text(original)}"
    return "Fornecedor"


def label_entidade(entity_id: Optional[int], original: Optional[str] = None) -> str:
    if entity_id is not None:
        return f"Entidade {int(entity_id)}"
    if original:
        return f"Entidade {_stable_int_from_text(original)}"
    return "Entidade"


def label_conta(original: Optional[str] = None) -> str:
    if original:
        return f"Conta {_stable_int_from_text(original)}"
    return "Conta"


def _mask_free_text(key: str, value: str, parent: dict[str, Any]) -> Optional[str]:
    """Return masked text for free-text keys, or None to leave unchanged."""
    kl = key.lower()
    if kl not in _FREE_TEXT_KEYS:
        return None
    text = value.strip()
    if not text:
        return None
    # Pure filial nicknames ("VR 07", "VR07")
    if _FILIAL_NICK_RE.match(text):
        return label_filial(_first_int(parent, "id_filial", "branch_id"), text)
    # Bank / account lines embed filial codes
    if _ACCOUNTISH_RE.search(text):
        return label_conta(text)
    # Explicit account-ish keys always mask non-trivial values
    if kl in {"descricao_conta", "nome_conta", "conta", "conta_label"} and len(text) > 2:
        return label_conta(text)
    return None


def _mask_contact(key: str, value: Any) -> Any:
    if value is None:
        return value
    if not isinstance(value, str):
        value = str(value)
    text = value.strip()
    if not text or text in {"—", "-", "***"}:
        return "—"
    kl = key.lower()
    if "email" in kl or kl in {"mail", "e_mail"}:
        return _MASKED_EMAIL
    if "tel" in kl or "fone" in kl or "cel" in kl or "phone" in kl:
        return _MASKED_PHONE
    if "cnpj" in kl:
        return _MASKED_CNPJ
    if "cpf" in kl and "cnpj" not in kl:
        return _MASKED_CPF
    digits = re.sub(r"\D+", "", text)
    if len(digits) >= 14:
        return _MASKED_CNPJ
    if len(digits) == 11 and "cpf" in kl:
        return _MASKED_CPF
    if "cpf" in kl or "cnpj" in kl or "doc" in kl:
        return "—"
    return "—"


def _first_int(d: dict[str, Any], *keys: str) -> Optional[int]:
    for key in keys:
        if key in d and d[key] is not None:
            try:
                return int(d[key])
            except (TypeError, ValueError):
                continue
        # case-insensitive fallback for RealDict / mixed casing
        lower_map = {str(k).lower(): k for k in d.keys()}
        real = lower_map.get(key.lower())
        if real is not None and d[real] is not None:
            try:
                return int(d[real])
            except (TypeError, ValueError):
                continue
    return None


def _mask_scalar_for_key(key: str, value: Any, parent: Optional[dict[str, Any]]) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return value

    kl = key.lower()
    if kl in _NEVER_MASK_KEYS:
        return value

    parent = parent or {}

    if kl in _CONTACT_KEYS:
        return _mask_contact(kl, value)

    if kl in _COMPANY_KEYS:
        return label_empresa(_first_int(parent, "id_empresa", "tenant_id"), text)

    if kl in _FILIAL_KEYS:
        return label_filial(_first_int(parent, "id_filial", "branch_id"), text)

    if kl in _PERSON_KEYS or kl in _SESSION_PERSON_KEYS:
        pid = _first_int(
            parent,
            "id_funcionario",
            "id_vendedor",
            "id_operador",
            "id_usuario",
            "user_id",
            "id_frentista",
        )
        return label_pessoa(pid, text)

    if kl in _CLIENT_KEYS:
        # entidade_de / entidade_para are generic counterparties
        if kl in {"entidade", "entidade_nome", "nome_entidade", "entidade_de", "entidade_para"}:
            eid = _first_int(parent, "id_entidade", "id_entidade_de", "id_entidade_para", "id_cliente")
            if kl.startswith("entidade"):
                return label_entidade(eid, text)
        cid = _first_int(parent, "id_cliente", "id_entidade", "cliente_id")
        return label_cliente(cid, text)

    if kl in _SUPPLIER_KEYS:
        return label_fornecedor(_first_int(parent, "id_fornecedor", "id_entidade"), text)

    free = _mask_free_text(kl, text, parent)
    if free is not None:
        return free

    # Catch-all for *\_nome / *\_label identity fields not yet listed
    if kl.endswith("_nome") or kl.endswith("_name") or kl.endswith("_label"):
        if any(
            stem in kl
            for stem in (
                "produto",
                "grupo",
                "categoria",
                "forma",
                "pagamento",
                "tier",
                "turno",
                "nivel",
                "role",
                "payment",
            )
        ):
            return value
        if "filial" in kl or "branch" in kl:
            return label_filial(_first_int(parent, "id_filial", "branch_id"), text)
        if "empresa" in kl or "company" in kl or "tenant" in kl:
            return label_empresa(_first_int(parent, "id_empresa", "tenant_id"), text)
        if "cliente" in kl or "entidade" in kl:
            return label_cliente(_first_int(parent, "id_cliente", "id_entidade", "cliente_id"), text)
        if "fornecedor" in kl or "supplier" in kl:
            return label_fornecedor(_first_int(parent, "id_fornecedor", "id_entidade"), text)
        # Generic UI labels that are not people
        if text.lower().startswith("sem ") or text in {"—", "-", "N/A"}:
            return text
        return label_pessoa(
            _first_int(
                parent,
                "id_funcionario",
                "id_vendedor",
                "id_operador",
                "id_usuario",
                "user_id",
                "id_frentista",
            ),
            text,
        )

    # Contextual bare "nome" — prefer person/cliente over filial when siblings exist
    if kl == "nome":
        if _first_int(parent, "id_funcionario", "id_vendedor", "id_operador", "id_usuario") is not None:
            return label_pessoa(
                _first_int(parent, "id_funcionario", "id_vendedor", "id_operador", "id_usuario"),
                text,
            )
        if _first_int(parent, "id_cliente", "id_entidade") is not None:
            return label_cliente(_first_int(parent, "id_cliente", "id_entidade"), text)
        if _first_int(parent, "id_filial") is not None:
            return label_filial(_first_int(parent, "id_filial"), text)
        if _first_int(parent, "id_produto", "id_grupo_produto") is not None:
            # product / group display names — not personal identity
            return value
        if _first_int(parent, "id_empresa") is not None and len(parent) <= 8:
            # compact company rows in selectors
            return label_empresa(_first_int(parent, "id_empresa"), text)
        # session / generic display name without sibling ids
        return label_pessoa(None, text)

    return value


def mask_identity_payload(data: Any) -> Any:
    """Recursively replace allowlisted identity fields. Mutates dicts/lists in place."""
    if not identity_mask_enabled():
        return data
    return _walk(data, parent=None)


def maybe_mask_identity(data: Any) -> Any:
    """Public entry used by routes that do not go through ``redact_sensitive``."""
    return mask_identity_payload(data)


def mask_branding_public(payload: dict[str, Any]) -> dict[str, Any]:
    """Force TorqMind default branding on Hom demo (no client logos/names)."""
    if not identity_mask_enabled():
        return payload
    out = dict(payload)
    id_empresa = out.get("id_empresa")
    try:
        eid = int(id_empresa) if id_empresa is not None else 0
    except (TypeError, ValueError):
        eid = 0
    out["background_url"] = None
    out["logo_url"] = None
    out["background_version"] = None
    out["logo_version"] = None
    out["uses_default"] = True
    out["display_name"] = label_empresa(eid if eid else None)
    return out


def _walk(obj: Any, parent: Optional[dict[str, Any]]) -> Any:
    if isinstance(obj, dict):
        for key, value in list(obj.items()):
            if isinstance(value, dict):
                _walk(value, parent=value)
            elif isinstance(value, list):
                _walk(value, parent=obj)
            else:
                masked = _mask_scalar_for_key(str(key), value, obj)
                if masked is not value:
                    obj[key] = masked
        return obj
    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                _walk(item, parent=item)
            elif isinstance(item, list):
                _walk(item, parent=parent)
        return obj
    return obj
