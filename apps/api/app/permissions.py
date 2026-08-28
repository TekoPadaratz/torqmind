"""Central screen/panel authorization for TorqMind BI.

Every BI route maps to a ``screen_key`` (menu) or panel key
(``menu.panel``). Access is granted based on:

1. **Role defaults** — platform_master / tenant_admin get all screens.
2. **Explicit permissions** — tenant_manager / tenant_viewer / tenant_kiosk
   require rows in ``auth.user_screen_permissions``.
3. **Sensitive-field redaction** — margin/profit/cost fields are stripped
   from API responses for roles without financial visibility.

Hierarchy:
- Menu keys (no ``parent``) appear in the product nav.
- Panel keys have ``parent`` = menu key (aba/painel dentro do menu).
- Unchecking a menu clears all of its panels (admin UI).
- Legacy rows with only the menu key expand to all of its panels at read time.

Usage in routes::

    from app.permissions import require_screen, redact_sensitive

    @router.get("/sales/overview")
    def sales_overview(
        ...,
        claims=Depends(get_current_claims),
        _screen=Depends(require_screen("sales")),
    ):
        data = build_payload(...)
        return redact_sensitive(data, claims)

    # Painel/aba:
    _screen=Depends(require_screen("profit_management.overview"))
"""
from __future__ import annotations

import copy
import logging
from typing import Any, Dict, List, Optional, Set

from fastapi import Depends, HTTPException

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Screen registry  (add new menus/panels here — no migration needed)
# ──────────────────────────────────────────────────────────────────────
# Contratos:
# - Menu = item de navegação (PRODUCT_LINKS / AppNav).
# - Panel = aba ou painel dentro do menu (chave ``parent.panel``).
# - Toda nova aba/painel DEVE ser registrada aqui + require_screen no endpoint
#   + checkbox no cadastro de usuário (árvore via screen_permission_tree()).
SCREEN_REGISTRY: Dict[str, Dict[str, Any]] = {
    # Legado: mantido para API/cache; oculto do menu e do cadastro de ACL.
    "dashboard_home": {
        "label": "Dashboard (legado)",
        "category": "BI",
        "has_sensitive": True,
        "nav_hidden": True,
    },
    "sales": {
        "label": "Vendas",
        "category": "Comercial",
        "has_sensitive": True,
    },
    "sales.overview": {
        "label": "Visão geral",
        "category": "Comercial",
        "parent": "sales",
        "has_sensitive": True,
    },
    "sales.evolution": {
        "label": "Evolução de vendas",
        "category": "Comercial",
        "parent": "sales",
        "has_sensitive": True,
    },
    "sales.hourly": {
        "label": "Vendas por hora",
        "category": "Comercial",
        "parent": "sales",
        "has_sensitive": True,
    },
    "sales.top": {
        "label": "Top vendas",
        "category": "Comercial",
        "parent": "sales",
        "has_sensitive": True,
    },
    "sales.abc": {
        "label": "Curva ABC",
        "category": "Comercial",
        "parent": "sales",
        "has_sensitive": True,
    },
    "cash": {
        "label": "Caixa",
        "category": "Operação",
        "has_sensitive": True,
    },
    "fraud": {
        "label": "Antifraude",
        "category": "Operação",
        "has_sensitive": False,
    },
    "fraud.core": {
        "label": "Cancelamentos e operadores",
        "category": "Operação",
        "parent": "fraud",
        "has_sensitive": False,
    },
    "fraud.risco_financeiro": {
        "label": "Risco financeiro / créditos",
        "category": "Operação",
        "parent": "fraud",
        "has_sensitive": False,
    },
    "fraud.credito_funcionario": {
        "label": "Crédito funcionário",
        "category": "Operação",
        "parent": "fraud",
        "has_sensitive": False,
    },
    "finance": {
        "label": "Financeiro",
        "category": "Financeiro",
        "has_sensitive": True,
    },
    "finance.overview": {
        "label": "Geral (Pagar × Receber)",
        "category": "Financeiro",
        "parent": "finance",
        "has_sensitive": True,
    },
    "finance.payable": {
        "label": "Contas a pagar",
        "category": "Financeiro",
        "parent": "finance",
        "has_sensitive": True,
    },
    "finance.receivable": {
        "label": "Contas a receber",
        "category": "Financeiro",
        "parent": "finance",
        "has_sensitive": True,
    },
    "finance.cheques": {
        "label": "Controle de cheques",
        "category": "Financeiro",
        "parent": "finance",
        "has_sensitive": True,
    },
    "finance.budget": {
        "label": "Gestão orçamentária",
        "category": "Financeiro",
        "parent": "finance",
        "has_sensitive": True,
    },
    "finance.despesas": {
        "label": "Despesas",
        "category": "Financeiro",
        "parent": "finance",
        "has_sensitive": True,
    },
    "customers": {
        "label": "Clientes",
        "category": "Comercial",
        "has_sensitive": False,
    },
    "inventory": {
        "label": "Estoque de combustível",
        "category": "Comercial",
        "has_sensitive": True,
    },
    "fuel_loss": {
        "label": "Movimentações de Combustível",
        "category": "Operação",
        "has_sensitive": False,
    },
    "product_management": {
        "label": "Gestão de Produtos",
        "category": "Operação",
        "has_sensitive": True,
    },
    "competitor_pricing": {
        "label": "Preço Concorrente",
        "category": "Comercial",
        "has_sensitive": False,
    },
    "competitor_pricing.register": {
        "label": "Registrar preços",
        "category": "Comercial",
        "parent": "competitor_pricing",
        "has_sensitive": False,
    },
    "competitor_pricing.history": {
        "label": "Histórico",
        "category": "Comercial",
        "parent": "competitor_pricing",
        "has_sensitive": False,
    },
    "competitor_pricing.comparison": {
        "label": "Comparativo",
        "category": "Comercial",
        "parent": "competitor_pricing",
        "has_sensitive": False,
    },
    "goals_team": {
        "label": "Metas",
        "category": "Comercial",
        "has_sensitive": True,
    },
    "goals_team.metas": {
        "label": "Metas",
        "category": "Comercial",
        "parent": "goals_team",
        "has_sensitive": True,
    },
    "goals_team.comissoes": {
        "label": "Vendedor",
        "category": "Comercial",
        "parent": "goals_team",
        "has_sensitive": True,
    },
    "goals_team.gerente": {
        "label": "Gerente",
        "category": "Comercial",
        "parent": "goals_team",
        "has_sensitive": True,
        "requires_sensitive_role": True,
    },
    "goals_team.config": {
        "label": "Configuração",
        "category": "Comercial",
        "parent": "goals_team",
        "has_sensitive": True,
    },
    "goals_team.orcamento": {
        "label": "Orçamento (legado)",
        "category": "Comercial",
        "parent": "goals_team",
        "has_sensitive": True,
    },
    "team": {
        "label": "Equipe",
        "category": "Comercial",
        "has_sensitive": True,
    },
    "team.custos": {
        "label": "Custo do funcionário",
        "category": "Comercial",
        "parent": "team",
        "has_sensitive": True,
    },
    "profit_management": {
        "label": "Gestão de Lucro",
        "category": "Financeiro",
        "has_sensitive": True,
    },
    "profit_management.overview": {
        "label": "Visão Geral (DRE)",
        "category": "Financeiro",
        "parent": "profit_management",
        "has_sensitive": True,
    },
    "profit_management.products": {
        "label": "Produtos",
        "category": "Financeiro",
        "parent": "profit_management",
        "has_sensitive": True,
    },
    "profit_management.repricing": {
        "label": "Oportunidades",
        "category": "Financeiro",
        "parent": "profit_management",
        "has_sensitive": True,
    },
    "profit_management.solvencia": {
        "label": "Solvência",
        "category": "Financeiro",
        "parent": "profit_management",
        "has_sensitive": True,
        "requires_sensitive_role": True,
    },
    "profit_management.anp": {
        "label": "Compliance ANP",
        "category": "Financeiro",
        "parent": "profit_management",
        "has_sensitive": True,
        "requires_sensitive_role": True,
    },
    "platform": {
        "label": "Plataforma",
        "category": "Administração Global",
        "platform_only": True,
    },
    "user_management": {
        "label": "Usuários",
        "category": "Administração",
        "platform_only": True,
    },
    "assistant": {
        "label": "Assistente TorqMind",
        "category": "Produto",
        "has_sensitive": False,
        "nav_hidden": True,
    },
    "tv_sales_hourly": {
        "label": "TV – Vendas por Hora",
        "category": "TV",
        "has_sensitive": False,
        "kiosk_only": True,
    },
    "tv_sales_ranking": {
        "label": "TV – Ranking Vendedores",
        "category": "TV",
        "has_sensitive": False,
        "kiosk_only": True,
    },
}


def is_menu_screen(screen_key: str) -> bool:
    meta = SCREEN_REGISTRY.get(screen_key) or {}
    return bool(meta) and not meta.get("parent") and not meta.get("platform_only") and not meta.get("nav_hidden")


def is_panel_screen(screen_key: str) -> bool:
    return bool((SCREEN_REGISTRY.get(screen_key) or {}).get("parent"))


def screen_children(parent_key: str) -> List[str]:
    return sorted(
        k for k, v in SCREEN_REGISTRY.items() if v.get("parent") == parent_key
    )


def expand_screen_permissions(raw: Set[str]) -> Set[str]:
    """Normalize ACL set for runtime checks.

    - Parent present without any of its panels → grant all panels (legado).
    - Any panel present → ensure parent is present (nav + pré-requisito).
    - dashboard_home (legado) → garante sales (nova home do produto).
    """
    result = {k for k in raw if k in SCREEN_REGISTRY}
    # Migração IA: Dashboard Geral removido do menu; quem tinha só ele passa a Vendas.
    if "dashboard_home" in result and "sales" not in result:
        result.add("sales")
    parents = {
        k for k, v in SCREEN_REGISTRY.items()
        if not v.get("parent") and not v.get("platform_only") and not v.get("kiosk_only")
    }
    for parent in parents:
        children = screen_children(parent)
        if not children:
            continue
        has_parent = parent in result
        has_child = any(c in result for c in children)
        if has_parent and not has_child:
            result.update(children)
        elif has_child and not has_parent:
            result.add(parent)
    # Assistente: disponível a quem já tem qualquer tela de produto (não TV).
    # nav_hidden — não aparece no menu; evita 403 em managers com ACL antiga.
    product_keys = {
        k for k in result
        if k in SCREEN_REGISTRY
        and not (SCREEN_REGISTRY[k] or {}).get("kiosk_only")
        and not (SCREEN_REGISTRY[k] or {}).get("platform_only")
        and k != "assistant"
    }
    if product_keys:
        result.add("assistant")
    return result


def screen_permission_tree(*, include_kiosk: bool = False, include_platform: bool = False) -> List[Dict[str, Any]]:
    """Árvore menu → painéis para o cadastro de usuário / contrato FE."""
    menus: List[Dict[str, Any]] = []
    for key, meta in SCREEN_REGISTRY.items():
        if meta.get("parent"):
            continue
        if meta.get("nav_hidden"):
            continue
        if meta.get("platform_only") and not include_platform:
            continue
        if meta.get("kiosk_only") and not include_kiosk:
            continue
        children = []
        for child_key in screen_children(key):
            child = SCREEN_REGISTRY[child_key]
            if child.get("nav_hidden"):
                continue
            children.append(
                {
                    "key": child_key,
                    "label": child.get("label") or child_key,
                    "requires_sensitive_role": bool(child.get("requires_sensitive_role")),
                }
            )
        menus.append(
            {
                "key": key,
                "label": meta.get("label") or key,
                "category": meta.get("category"),
                "kiosk_only": bool(meta.get("kiosk_only")),
                "panels": children,
            }
        )
    menus.sort(key=lambda m: (m.get("category") or "", m.get("label") or ""))
    return menus


# Keys available to each role *by default* (without explicit DB rows).
# tenant_manager / tenant_viewer / tenant_kiosk must have explicit rows.
_ALL_PRODUCT_SCREENS = {
    k for k, v in SCREEN_REGISTRY.items()
    if not v.get("platform_only") and not v.get("kiosk_only")
}

_ALL_SCREENS = set(SCREEN_REGISTRY.keys())

_TV_SCREENS = {
    k for k, v in SCREEN_REGISTRY.items()
    if v.get("kiosk_only")
}

_MENU_PRODUCT_SCREENS = {
    k for k in _ALL_PRODUCT_SCREENS if is_menu_screen(k)
}

ROLE_DEFAULT_SCREENS: Dict[str, Set[str]] = {
    "platform_master": _ALL_SCREENS,
    "platform_admin": _ALL_SCREENS - {"platform"},  # no finance but has ops
    "product_global": _ALL_PRODUCT_SCREENS,
    "channel_admin": _ALL_PRODUCT_SCREENS | {"user_management"},
    "tenant_admin": _ALL_PRODUCT_SCREENS,
}
# tenant_manager, tenant_viewer, tenant_kiosk → from DB only

MODULE_TIER_KEYS: tuple[str, ...] = ("essencial", "profissional", "gestao", "intelligence")

_MODULE_TIER_ESSENCIAL: Set[str] = {
    "sales",
    "sales.overview",
    "sales.evolution",
    "sales.hourly",
    "sales.top",
    "sales.abc",
    "customers",
    "team",
    "inventory",
    "competitor_pricing",
    "competitor_pricing.register",
    "competitor_pricing.history",
    "competitor_pricing.comparison",
}

_MODULE_TIER_PROFISSIONAL_EXTRA: Set[str] = {
    "finance",
    "finance.overview",
    "finance.payable",
    "finance.receivable",
    "finance.cheques",
    "cash",
    "goals_team",
    "goals_team.metas",
}

_MODULE_TIER_GESTAO_EXTRA: Set[str] = {
    "finance.despesas",
    "finance.budget",
    "profit_management",
    "profit_management.overview",
    "profit_management.products",
    "profit_management.repricing",
    "profit_management.solvencia",
    "profit_management.anp",
    "fraud",
    "fraud.core",
    "fraud.risco_financeiro",
    "fraud.credito_funcionario",
    "team.custos",
    "goals_team.comissoes",
    "goals_team.gerente",
    "goals_team.config",
    "fuel_loss",
}

MODULE_TIER_LABELS: Dict[str, str] = {
    "essencial": "Essencial",
    "profissional": "Profissional",
    "gestao": "Gestão",
    "intelligence": "Gestão + Intelligence",
}

MODULE_TIER_PRESETS_RAW: Dict[str, Set[str]] = {
    "essencial": set(_MODULE_TIER_ESSENCIAL),
    "profissional": set(_MODULE_TIER_ESSENCIAL) | _MODULE_TIER_PROFISSIONAL_EXTRA,
    "gestao": (
        set(_MODULE_TIER_ESSENCIAL)
        | _MODULE_TIER_PROFISSIONAL_EXTRA
        | _MODULE_TIER_GESTAO_EXTRA
    ),
    "intelligence": (
        set(_MODULE_TIER_ESSENCIAL)
        | _MODULE_TIER_PROFISSIONAL_EXTRA
        | _MODULE_TIER_GESTAO_EXTRA
    ),
}


def normalize_module_tier(tier: str | None) -> str:
    key = str(tier or "").strip().lower()
    if key in MODULE_TIER_PRESETS_RAW:
        return key
    return "essencial"


def module_tier_preset_screens(tier: str | None) -> List[str]:
    """Telas do pacote (expandidas) para pré-marcar ACL de usuário."""
    key = normalize_module_tier(tier)
    return sorted(expand_screen_permissions(MODULE_TIER_PRESETS_RAW[key]))


def module_tier_catalog() -> List[Dict[str, Any]]:
    return [
        {
            "key": key,
            "label": MODULE_TIER_LABELS.get(key, key),
            "screens": module_tier_preset_screens(key),
        }
        for key in MODULE_TIER_KEYS
    ]


# (tenant_manager defaults — ver default_explicit_screen_permissions)


def default_explicit_screen_permissions(role: str) -> List[str]:
    """Permissões iniciais para roles que gravam no DB.

    - tenant_manager / tenant_viewer → todos os menus + painéis do produto
    - tenant_kiosk → telas TV
    Roles altas (master/admin/owner) não usam esta lista: vêm de ROLE_DEFAULT_SCREENS.
    """
    if role == "tenant_kiosk":
        return sorted(_TV_SCREENS)
    if role in {"tenant_manager", "tenant_viewer"}:
        # Comissão de gerente é opt-in: funcionários veem só o relatório de vendedor.
        return sorted(k for k in _ALL_PRODUCT_SCREENS if k != "goals_team.gerente")
    return []

# ──────────────────────────────────────────────────────────────────────
# Sensitive field names (lowercase) to redact for non-financial roles
# ──────────────────────────────────────────────────────────────────────
SENSITIVE_FIELD_NAMES: Set[str] = {
    "margem",
    "margem_total",
    "s_margem",
    "margin",
    "lucro",
    "profit",
    "cmv",
    "custo",
    "custo_medio",
    "custo_total",
    "custo_unitario",
    "custo_estoque",
    "cost",
    "markup",
    "rentabilidade",
    "rentab",
    "margem_acumulada",
    "margem_percentual",
    "margin_10d",
    "margem_score",
    "profit_margin",
    "gross_margin",
    # Folha / custo de pessoal (Equipe — custo do funcionário)
    "vales",
    "horas_extras",
}

# Stems for substring-based redaction — any key whose lowercased name
# contains one of these stems is treated as sensitive.
_SENSITIVE_STEMS: tuple[str, ...] = (
    "margem", "margin", "lucro", "profit", "cmv",
    "custo", "cost", "markup", "rentab",
    # Folha/pessoal: salario_*, rateio_*, *_overhead_*, total_pessoal_*, folha_*
    "salario", "rateio", "overhead", "pessoal", "folha",
)

# Roles that can see sensitive financial data
_FINANCIAL_ROLES: Set[str] = {
    "platform_master",
    "platform_admin",
    "product_global",
    "tenant_admin",
}


def user_can_view_sensitive_financials(user_role: str, user_flag: bool | None = None) -> bool:
    if (user_role or "") in _FINANCIAL_ROLES:
        return True
    return bool(user_flag)


# ──────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────

def get_allowed_screens(claims: dict[str, Any]) -> Set[str]:
    """Return the set of screen_keys this user can access (menus + panels).

    For platform/admin/owner roles the default set is returned.
    For tenant_manager/viewer/kiosk the set is read from the JWT claims
    which were populated during session building (already expanded).
    """
    cached = claims.get("allowed_screens")
    if cached is not None:
        return expand_screen_permissions(set(cached))

    user_role: str = claims.get("user_role") or ""
    defaults = ROLE_DEFAULT_SCREENS.get(user_role)
    if defaults is not None:
        return set(defaults)

    # Fallback: should not happen if session builder ran correctly
    return set()


def can_access_screen(claims: dict[str, Any], screen_key: str) -> bool:
    return screen_key in get_allowed_screens(claims)


def can_view_sensitive_financials(claims: dict[str, Any]) -> bool:
    """True if the user may see margin / profit / cost fields."""
    cached = claims.get("can_view_sensitive_financials")
    if cached is not None:
        return bool(cached)
    role = claims.get("user_role") or ""
    if role in _FINANCIAL_ROLES:
        return True
    user_flag = claims.get("user_can_view_sensitive_financials")
    if user_flag is not None:
        return bool(user_flag)
    return False


def is_kiosk_user(claims: dict[str, Any]) -> bool:
    return (claims.get("user_role") or "") == "tenant_kiosk"


def resolve_default_route(claims: dict[str, Any]) -> str:
    """Determine the home page path for this user."""
    user_role = claims.get("user_role") or ""

    if user_role == "tenant_kiosk":
        screens = get_allowed_screens(claims)
        if "tv_sales_hourly" in screens:
            return "/tv/sales-hourly"
        if "tv_sales_ranking" in screens:
            return "/tv/sales-ranking"
        return "/tv"

    if user_role in _FINANCIAL_ROLES or user_role in {"tenant_manager", "tenant_viewer"}:
        screens = get_allowed_screens(claims)
        # First product screen in IA order (Comercial → Operação → Financeiro)
        ordered = [
            "sales", "customers", "equipe", "team", "inventory", "competitor_pricing", "goals_team",
            "cash", "fraud", "fuel_loss",
            "finance", "profit_management",
            "dashboard_home",  # legado
        ]
        for key in ordered:
            if key in screens:
                route_map = {
                    "dashboard_home": "/sales",
                    "sales": "/sales",
                    "cash": "/cash",
                    "fraud": "/fraud",
                    "customers": "/customers",
                    "finance": "/finance",
                    "profit_management": "/profit-management",
                    "inventory": "/inventory",
                    "fuel_loss": "/fuel-loss",
                    "competitor_pricing": "/pricing",
                    "goals_team": "/goals",
                    "team": "/team",
                    "equipe": "/team",
                }
                return route_map[key]

    return "/sales"


# ──────────────────────────────────────────────────────────────────────
# Sensitive field redaction
# ──────────────────────────────────────────────────────────────────────

def redact_sensitive(data: Any, claims: dict[str, Any]) -> Any:
    """Recursively remove / zero-out sensitive financial fields.

    The sensitive-field redaction is only applied when
    ``can_view_sensitive_financials(claims)`` is False. Text hygiene
    (mojibake repair for corrupted source strings) runs for **all** roles.
    Returns the (possibly modified) data — mutates in place for dicts/lists.
    """
    _sanitize_text(data)
    if can_view_sensitive_financials(claims):
        return data
    return _redact(data)


# ──────────────────────────────────────────────────────────────────────
# Text hygiene — repair mojibake coming from corrupted source rows
# ──────────────────────────────────────────────────────────────────────
# Some rows in the client's ERP (Xpert, CP1252 varchar columns) were written
# with UTF-8 bytes stuffed into a Latin-1 column, producing double-encoded
# text (e.g. "13º Salário" -> "13Âº Salário", "Serviços" -> "ServiÃ§os").
# The repair is CONSERVATIVE: it only touches strings containing a mojibake
# marker and only accepts a fix that strictly reduces the mojibake score, so
# clean text (whose round-trip fails or does not improve) is never altered.

_MOJIBAKE_MARKERS: tuple[str, ...] = ("Ã", "Â", "â€", "Ë", "Ð", "Ñ")


def _mojibake_score(text: str) -> int:
    """Rough count of mojibake marker occurrences in *text*."""
    return sum(text.count(m) for m in _MOJIBAKE_MARKERS)


def _try_fix_once(text: str) -> str:
    """Attempt a single mojibake round-trip; return best improving candidate."""
    best = text
    best_score = _mojibake_score(text)
    for codec in ("cp1252", "latin-1"):
        try:
            candidate = text.encode(codec).decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
            continue
        if candidate != text:
            cand_score = _mojibake_score(candidate)
            if cand_score < best_score:
                best, best_score = candidate, cand_score
    return best


def fix_mojibake(text: str) -> str:
    """Repair double-encoded (mojibake) text. Safe on clean strings.

    Only strings containing a mojibake marker are processed; the repair is
    accepted only when it strictly reduces the mojibake marker count, so clean
    accented text (e.g. "CARTÃO", "São") is preserved untouched.
    """
    if not text or not any(m in text for m in _MOJIBAKE_MARKERS):
        return text
    fixed = text
    for _ in range(3):  # handle up to triple-encoded strings
        candidate = _try_fix_once(fixed)
        if candidate == fixed:
            break
        fixed = candidate
        if not any(m in fixed for m in _MOJIBAKE_MARKERS):
            break
    return fixed


def _sanitize_text(obj: Any) -> Any:
    """Recursively repair mojibake in every string value (mutates in place)."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str):
                fixed = fix_mojibake(value)
                if fixed is not value:
                    obj[key] = fixed
            else:
                _sanitize_text(value)
    elif isinstance(obj, list):
        for idx, item in enumerate(obj):
            if isinstance(item, str):
                fixed = fix_mojibake(item)
                if fixed is not item:
                    obj[idx] = fixed
            else:
                _sanitize_text(item)
    return obj


def _is_sensitive_key(key: str) -> bool:
    """Return True if *key* is a known sensitive financial field."""
    lower = key.lower()
    if lower in SENSITIVE_FIELD_NAMES:
        return True
    return any(stem in lower for stem in _SENSITIVE_STEMS)


def _redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        for key in list(obj.keys()):
            if _is_sensitive_key(key):
                obj[key] = None
            else:
                _redact(obj[key])
    elif isinstance(obj, list):
        for item in obj:
            _redact(item)
    return obj


# ──────────────────────────────────────────────────────────────────────
# FastAPI dependency: require_screen("screen_key")
# ──────────────────────────────────────────────────────────────────────

def require_screen(screen_key: str):
    """Return a FastAPI Depends-compatible callable that raises 403
    if the current user cannot access *screen_key*."""

    def _check(claims: dict[str, Any] = Depends(_get_claims_ref())):
        if not can_access_screen(claims, screen_key):
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "screen_access_denied",
                    "message": f"Acesso negado à tela '{screen_key}'.",
                    "screen_key": screen_key,
                },
            )

    return _check


def require_not_kiosk():
    """Return a FastAPI Depends-compatible callable that raises 403
    if the current user is a kiosk (tenant_kiosk) session."""

    def _check(claims: dict[str, Any] = Depends(_get_claims_ref())):
        if (claims.get("user_role") or "") == "tenant_kiosk":
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "kiosk_not_allowed",
                    "message": "Endpoint não disponível para modo TV.",
                },
            )

    return _check


def _get_claims_ref():
    """Lazy import to avoid circular dependency with deps.py."""
    from app.deps import get_current_claims
    return get_current_claims


# ──────────────────────────────────────────────────────────────────────
# Allowed screen_keys per role group (for validation on save)
# ──────────────────────────────────────────────────────────────────────

_ALLOWED_SCREENS_BY_ROLE: Dict[str, Set[str]] = {
    "tenant_kiosk": _TV_SCREENS,
    "tenant_manager": _ALL_PRODUCT_SCREENS,
    "tenant_viewer": _ALL_PRODUCT_SCREENS,
}


def validate_screen_permissions_for_role(role: str, screen_keys: List[str]) -> List[str]:
    """Validate that screen_keys are allowed for the given role.

    Raises HTTPException(422) if any disallowed key is found.
    Returns the validated (normalized) list.
    """
    normalized = normalize_screen_permissions_for_save(screen_keys)
    allowed = _ALLOWED_SCREENS_BY_ROLE.get(role)
    if allowed is None:
        return normalized  # admin/owner roles — no restriction

    disallowed = [k for k in normalized if k not in allowed]
    if disallowed:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "screen_permission_invalid",
                "message": f"Telas não permitidas para role '{role}': {', '.join(sorted(disallowed))}.",
                "disallowed": sorted(disallowed),
            },
        )
    return normalized


# ──────────────────────────────────────────────────────────────────────
# DB helpers  (used by session builder in repos_auth.py)
# ──────────────────────────────────────────────────────────────────────

def load_user_screen_permissions(conn, user_id: str) -> Set[str]:
    """Read explicit screen permissions from auth.user_screen_permissions."""
    cur = conn.execute(
        "SELECT screen_key FROM auth.user_screen_permissions WHERE user_id = %s::uuid",
        (user_id,),
    )
    return expand_screen_permissions({row["screen_key"] for row in cur.fetchall()})


def normalize_screen_permissions_for_save(screen_keys: List[str]) -> List[str]:
    """Persist only known keys; drop orphan panels whose menu is unchecked."""
    raw = {k for k in screen_keys if k in SCREEN_REGISTRY}
    cleaned: Set[str] = set()
    for key in raw:
        parent = (SCREEN_REGISTRY.get(key) or {}).get("parent")
        if parent and parent not in raw:
            continue
        cleaned.add(key)
    return sorted(cleaned)


def save_user_screen_permissions(conn, user_id: str, screen_keys: List[str]) -> None:
    """Replace all screen permissions for a user (within current transaction)."""
    keys = normalize_screen_permissions_for_save(screen_keys)
    conn.execute(
        "DELETE FROM auth.user_screen_permissions WHERE user_id = %s::uuid",
        (user_id,),
    )
    for key in keys:
        conn.execute(
            """INSERT INTO auth.user_screen_permissions (user_id, screen_key)
               VALUES (%s::uuid, %s)
               ON CONFLICT (user_id, screen_key) DO NOTHING""",
            (user_id, key),
        )
