"""Executor allowlisted — analytics somente via repos_analytics."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from datetime import date
from typing import Any, Callable, Optional

from app.intelligence.authz import reauthorize
from app.intelligence.capability_map.loader import list_intents
from app.intelligence.evidence import EvidenceStore
from app.intelligence.limits import get_limits
from app.intelligence.tools.registry import WRITE_BLOCKLIST, assert_no_write, get_tool
from app.permissions import can_access_screen, can_view_sensitive_financials, redact_sensitive


def _role(claims: dict[str, Any]) -> str:
    return str(claims.get("user_role") or claims.get("role") or "tenant_viewer")


def _hash_result(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40]


def _trim_rows(payload: Any, max_rows: int) -> Any:
    if isinstance(payload, list):
        return payload[:max_rows]
    if isinstance(payload, dict):
        out = dict(payload)
        for key in ("items", "rows", "data", "events", "products", "titles"):
            if isinstance(out.get(key), list):
                out[key] = out[key][:max_rows]
        return out
    return payload


def _call_analytics(fn_name: str, *args: Any, **kwargs: Any) -> Any:
    assert_no_write(fn_name)
    if fn_name in WRITE_BLOCKLIST:
        raise RuntimeError(f"blocked write: {fn_name}")
    from app import repos_analytics as repos_mart

    fn = getattr(repos_mart, fn_name, None)
    if fn is None or not callable(fn):
        raise AttributeError(f"analytics function unavailable: {fn_name}")
    return fn(*args, **kwargs)


def _mask_doc(doc: Any) -> str:
    digits = "".join(ch for ch in str(doc or "") if ch.isdigit())
    if len(digits) < 5:
        return "—"
    return f"***.***.{digits[-4:]}" if len(digits) >= 4 else "***"


def _handler_customer_search(args: dict[str, Any], claims: dict[str, Any], scope: dict[str, Any]) -> Any:
    search = str(args.get("customer_name") or args.get("search") or "").strip()
    result = _call_analytics(
        "customers_summary_paginated",
        _role(claims),
        int(scope["id_empresa"]),
        scope.get("id_filial"),
        page=1,
        page_size=5,
        search=search,
    )
    items = []
    for row in (result or {}).get("items") or []:
        items.append(
            {
                "nome": row.get("nome_cliente") or row.get("nome"),
                "documento_masked": _mask_doc(row.get("documento")),
                "ref": f"c:{row.get('id_cliente')}",
            }
        )
    return {"candidates": items, "total": (result or {}).get("total", len(items)), "search": search}


def _handler_unsupported(*_a: Any, **_k: Any) -> Any:
    return {
        "status": "unsupported",
        "message": "Esta consulta ainda não está disponível no Assistente. Use a tela correspondente do TorqMind.",
    }


def _handler_commissions_readonly(*_a: Any, **_k: Any) -> Any:
    return {
        "status": "navigate_only",
        "message": (
            "Comissões são consultadas na tela Metas → Vendedor. "
            "O assistente não altera comissões nem metas."
        ),
        "deep_link": "/goals?tab=comissoes",
    }


def _handler_profit_overview(args: dict[str, Any], claims: dict[str, Any], scope: dict[str, Any]) -> Any:
    # tenta funções read-only conhecidas via facade
    for name in ("profit_overview", "profit_management_overview", "dre_overview"):
        try:
            return _call_analytics(
                name,
                _role(claims),
                int(scope["id_empresa"]),
                scope.get("id_filial"),
                args.get("dt_ini"),
                args.get("dt_fim"),
            )
        except Exception:
            continue
    return {
        "status": "navigate_only",
        "message": "Para ver lucro e margem, abra a tela Gestão de Lucro.",
        "deep_link": "/profit-management",
    }


def _handler_anp(args: dict[str, Any], claims: dict[str, Any], scope: dict[str, Any]) -> Any:
    for name in ("anp_compliance_overview", "anp_reference_overview"):
        try:
            return _call_analytics(name, _role(claims), int(scope["id_empresa"]), scope.get("id_filial"))
        except Exception:
            continue
    return {
        "status": "navigate_only",
        "message": "Consulte o Compliance ANP na tela Gestão de Lucro.",
        "deep_link": "/profit-management",
    }


def _handler_capabilities(args: dict[str, Any], claims: dict[str, Any], scope: dict[str, Any]) -> Any:
    sensitive = can_view_sensitive_financials(claims)
    items = []
    for intent in list_intents():
        if intent.get("requires_sensitive_role") or intent.get("requires_sensitive"):
            if not sensitive:
                continue
        screen = intent.get("screen_key")
        if screen and screen != "assistant" and not can_access_screen(claims, str(screen)):
            continue
        syns = intent.get("synonyms") or []
        label = syns[0] if syns else intent.get("intent_id")
        items.append(
            {
                "intent_id": intent.get("intent_id"),
                "label": label,
                "example": f"Ex.: {syns[0]}" if syns else None,
                "deep_link": intent.get("deep_link_key"),
            }
        )
    return {"capabilities": items[:40]}


def _handler_navigation(args: dict[str, Any], claims: dict[str, Any], scope: dict[str, Any]) -> Any:
    target = str(args.get("target") or args.get("query") or "").strip().lower()
    mapping = {
        "vendas": "/sales",
        "clientes": "/customers",
        "financeiro": "/finance",
        "caixa": "/cash",
        "antifraude": "/fraud",
        "metas": "/goals",
        "equipe": "/team",
        "estoque": "/inventory",
        "preco": "/pricing",
        "lucro": "/profit-management",
    }
    for key, path in mapping.items():
        if key in target:
            return {"deep_link": path, "label": key}
    return {"deep_link": "/sales", "label": "vendas"}


def _handler_data_freshness(args: dict[str, Any], claims: dict[str, Any], scope: dict[str, Any]) -> Any:
    try:
        return _call_analytics("streaming_health", int(scope["id_empresa"]))
    except Exception as exc:
        return {"status": "error", "message": "Não foi possível verificar o frescor dos dados.", "detail": str(exc)[:120]}


def _handler_goals(args: dict[str, Any], claims: dict[str, Any], scope: dict[str, Any]) -> Any:
    today = args.get("dt_fim") or date.today()
    try:
        goals = _call_analytics("goals_today", _role(claims), int(scope["id_empresa"]), scope.get("id_filial"), today)
    except Exception:
        goals = None
    try:
        pace = _call_analytics(
            "monthly_goal_projection", _role(claims), int(scope["id_empresa"]), scope.get("id_filial"), today
        )
    except Exception:
        pace = None
    return {"goals_today": goals, "pace": pace}


def _handler_team(args: dict[str, Any], claims: dict[str, Any], scope: dict[str, Any]) -> Any:
    for name in ("sales_top_employees", "leaderboard_employees"):
        try:
            return _call_analytics(
                name,
                _role(claims),
                int(scope["id_empresa"]),
                scope.get("id_filial"),
                args.get("dt_ini"),
                args.get("dt_fim"),
                10,
            )
        except TypeError:
            try:
                return _call_analytics(
                    name,
                    _role(claims),
                    int(scope["id_empresa"]),
                    scope.get("id_filial"),
                    args.get("dt_ini"),
                    args.get("dt_fim"),
                )
            except Exception:
                continue
        except Exception:
            continue
    return {"items": []}


def _handler_risk_events(args: dict[str, Any], claims: dict[str, Any], scope: dict[str, Any]) -> Any:
    for name in ("risk_last_events", "fraud_last_events"):
        try:
            return _call_analytics(name, _role(claims), int(scope["id_empresa"]), scope.get("id_filial"))
        except Exception:
            continue
    return {"items": []}


def _handler_credit(args: dict[str, Any], claims: dict[str, Any], scope: dict[str, Any]) -> Any:
    for name in ("fraud_credito_funcionario", "fraud_lancamentos_creditos"):
        try:
            return _call_analytics(name, _role(claims), int(scope["id_empresa"]), scope.get("id_filial"))
        except Exception:
            continue
    return {"items": []}


def _handler_cheques(args: dict[str, Any], claims: dict[str, Any], scope: dict[str, Any]) -> Any:
    try:
        return _call_analytics(
            "cheques_pendentes_overview",
            _role(claims),
            int(scope["id_empresa"]),
            scope.get("id_filial"),
        )
    except Exception:
        return {
            "status": "unsupported",
            "message": "Consulta de cheques indisponível no momento. Abra Financeiro → Cheques.",
            "deep_link": "/finance?view=cheques",
        }


def _handler_open_titles(args: dict[str, Any], claims: dict[str, Any], scope: dict[str, Any]) -> Any:
    tipo = int(args.get("title_tipo") or args.get("tipo") or 1)
    q = str(args.get("customer_name") or args.get("search") or args.get("q") or "").strip() or None
    return _call_analytics(
        "finance_titles_overview",
        _role(claims),
        int(scope["id_empresa"]),
        scope.get("id_filial"),
        tipo,
        args.get("dt_ini"),
        args.get("dt_fim"),
        q=q,
        page=1,
        page_size=50,
    )


def _handler_finance_titles(args: dict[str, Any], claims: dict[str, Any], scope: dict[str, Any]) -> Any:
    tipo = int(args.get("title_tipo") or args.get("tipo") or 0)
    q = str(args.get("customer_name") or args.get("search") or args.get("q") or "").strip() or None
    return _call_analytics(
        "finance_titles_overview",
        _role(claims),
        int(scope["id_empresa"]),
        scope.get("id_filial"),
        tipo,
        args.get("dt_ini"),
        args.get("dt_fim"),
        q=q,
        page=1,
        page_size=50,
    )


def _playbook_handler(playbook_id: str) -> Callable[..., Any]:
    def _run(args: dict[str, Any], claims: dict[str, Any], scope: dict[str, Any]) -> Any:
        from app.intelligence.playbooks.engine import run_playbook

        result = run_playbook(
            playbook_id,
            context={
                "id_empresa": scope.get("id_empresa"),
                "id_filial": scope.get("id_filial"),
                "dt_ini": args.get("dt_ini"),
                "dt_fim": args.get("dt_fim"),
            },
            can_view_profit=can_view_sensitive_financials(claims),
        )
        if not result:
            return {"status": "unsupported", "message": "Plano de ação não disponível."}
        return result

    return _run


_HANDLERS: dict[str, Callable[..., Any]] = {
    "customer_search": _handler_customer_search,
    "unsupported": _handler_unsupported,
    "commissions_readonly": _handler_commissions_readonly,
    "profit_overview": _handler_profit_overview,
    "anp_reference": _handler_anp,
    "assistant_capabilities": _handler_capabilities,
    "navigation_resolve": _handler_navigation,
    "data_freshness": _handler_data_freshness,
    "goals_overview": _handler_goals,
    "team_overview": _handler_team,
    "risk_events": _handler_risk_events,
    "credit_sales": _handler_credit,
    "cheques_or_unsupported": _handler_cheques,
    "open_titles": _handler_open_titles,
    "finance_titles": _handler_finance_titles,
    "playbook_revenue_drop": _playbook_handler("revenue_drop"),
    "playbook_delinquency": _playbook_handler("delinquency_priority"),
    "playbook_mix": _playbook_handler("mix_shift"),
    "playbook_goals": _playbook_handler("goals_pace"),
    "playbook_idle_hours": _playbook_handler("idle_hours"),
}


def execute_tool(
    tool_name: str,
    args: dict[str, Any] | None,
    claims: dict[str, Any],
    scope: dict[str, Any],
    *,
    evidence: EvidenceStore | None = None,
) -> dict[str, Any]:
    """Executa tool allowlisted. Nunca passa SQL controlado pelo usuário."""
    started = time.perf_counter()
    spec = get_tool(tool_name)
    if not spec:
        return {
            "status": "error",
            "tool_name": tool_name,
            "error": "tool_not_allowlisted",
            "evidence_id": None,
        }

    ok, reason = reauthorize(claims, spec)
    if not ok:
        return {
            "status": "forbidden",
            "tool_name": tool_name,
            "error": reason,
            "evidence_id": None,
            "latency_ms": int((time.perf_counter() - started) * 1000),
        }

    safe_args = dict(args or {})
    # força escopo do servidor
    safe_args["id_empresa"] = int(scope["id_empresa"])
    if "id_filial" not in safe_args:
        safe_args["id_filial"] = scope.get("id_filial")
    # remove qualquer tentativa de SQL
    for bad in ("sql", "query_sql", "raw_sql", "statement"):
        safe_args.pop(bad, None)

    limits = get_limits()
    max_rows = int(spec.get("max_rows") or limits.max_tool_rows)
    status = "ok"
    payload: Any = None
    try:
        handler_name = spec.get("handler")
        if handler_name:
            handler = _HANDLERS.get(str(handler_name))
            if not handler:
                raise RuntimeError(f"handler missing: {handler_name}")
            payload = handler(safe_args, claims, scope)
        else:
            fn_name = str(spec.get("analytics_fn") or "")
            assert_no_write(fn_name)
            call_args = [
                _role(claims),
                int(scope["id_empresa"]),
                scope.get("id_filial"),
            ]
            if safe_args.get("dt_ini") is not None and safe_args.get("dt_fim") is not None:
                call_args.extend([safe_args["dt_ini"], safe_args["dt_fim"]])
            payload = _call_analytics(fn_name, *call_args)
        if isinstance(payload, dict) and payload.get("status") in {"unsupported", "navigate_only"}:
            status = str(payload["status"])
        payload = _trim_rows(payload, max_rows)
        payload = redact_sensitive(payload, claims)
    except Exception as exc:
        status = "error"
        payload = {"error": str(exc)[:200]}

    evidence_id = None
    store = evidence or EvidenceStore()
    if payload is not None and status in {"ok", "navigate_only", "unsupported"}:
        evidence_id = store.register(payload, source=tool_name)

    latency_ms = int((time.perf_counter() - started) * 1000)
    return {
        "status": status,
        "tool_name": tool_name,
        "tool_version": spec.get("version"),
        "result": payload,
        "result_hash": _hash_result(payload),
        "evidence_id": evidence_id,
        "latency_ms": latency_ms,
        "row_count": (
            len(payload)
            if isinstance(payload, list)
            else len(payload.get("items") or payload.get("candidates") or [])
            if isinstance(payload, dict)
            else None
        ),
        "request_id": str(uuid.uuid4()),
    }
