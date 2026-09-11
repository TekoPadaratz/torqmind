"""Investigação determinística de variação de vendas (Phase 3 — jornada 1).

Somente leitura via marts ClickHouse já usadas pelo BI. Sem SQL de LLM.
Distingue contribuição quantitativa, hipótese e (nunca) causa comprovada.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.business_time import business_today
from app.db_clickhouse import query_dict
from app.repos_mart_realtime import MART_RT_DB, _branch_clause, _date_range_filter

MAX_PERIOD_DAYS = 90
TOP_FACTORS = 5


def _as_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _period_days(dt_ini: date, dt_fim: date) -> int:
    return max(1, (dt_fim - dt_ini).days + 1)


def prior_equal_period(dt_ini: date, dt_fim: date) -> Tuple[date, date]:
    """Janela imediatamente anterior com a mesma duração em dias civis."""
    days = _period_days(dt_ini, dt_fim)
    prior_fim = dt_ini - timedelta(days=1)
    prior_ini = prior_fim - timedelta(days=days - 1)
    return prior_ini, prior_fim


def _period_label(dt_ini: date, dt_fim: date) -> str:
    if dt_ini == dt_fim:
        return dt_ini.isoformat()
    return f"{dt_ini.isoformat()} → {dt_fim.isoformat()}"


def _totals(
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
) -> Dict[str, Any]:
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)
    rows = query_dict(
        f"""
        SELECT
          sum(faturamento) AS faturamento,
          sum(qtd_vendas) AS qtd_vendas,
          sum(valor_cancelado) AS valor_cancelado,
          count() AS n_rows,
          max(published_at) AS last_updated
        FROM {MART_RT_DB}.sales_daily_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          {date_range}
          {filial}
        """,
        parameters={"id_empresa": int(id_empresa)},
    )
    row = rows[0] if rows else {}
    n_rows = int(row.get("n_rows") or 0)
    return {
        "faturamento": _as_float(row.get("faturamento")),
        "qtd_vendas": int(row.get("qtd_vendas") or 0),
        "valor_cancelado": _as_float(row.get("valor_cancelado")),
        "has_data": n_rows > 0,
        "n_rows": n_rows,
        "last_updated": row.get("last_updated"),
    }


def _by_filial(
    id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date
) -> List[Dict[str, Any]]:
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)
    return query_dict(
        f"""
        SELECT
          id_filial,
          sum(faturamento) AS faturamento,
          sum(qtd_vendas) AS qtd_vendas
        FROM {MART_RT_DB}.sales_daily_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          {date_range}
          {filial}
        GROUP BY id_filial
        ORDER BY faturamento DESC
        LIMIT 50
        """,
        parameters={"id_empresa": int(id_empresa)},
    )


def _by_group(
    id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date
) -> List[Dict[str, Any]]:
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)
    return query_dict(
        f"""
        SELECT
          id_grupo_produto,
          any(nome_grupo) AS grupo_nome,
          sum(faturamento) AS faturamento
        FROM {MART_RT_DB}.sales_groups_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          {date_range}
          {filial}
        GROUP BY id_grupo_produto
        ORDER BY faturamento DESC
        LIMIT 30
        """,
        parameters={"id_empresa": int(id_empresa)},
    )


def _by_hour(
    id_empresa: int, id_filial: Any, dt_ini: date, dt_fim: date
) -> List[Dict[str, Any]]:
    filial = _branch_clause("id_filial", id_filial)
    date_range = _date_range_filter(dt_ini, dt_fim)
    return query_dict(
        f"""
        SELECT
          hora,
          sum(faturamento) AS faturamento
        FROM {MART_RT_DB}.sales_hourly_rt FINAL
        WHERE id_empresa = {{id_empresa:Int32}}
          {date_range}
          {filial}
        GROUP BY hora
        ORDER BY hora
        """,
        parameters={"id_empresa": int(id_empresa)},
    )


def _delta_map(
    current: List[Dict[str, Any]],
    prior: List[Dict[str, Any]],
    *,
    key: str,
    label_key: Optional[str],
    value_key: str = "faturamento",
) -> List[Dict[str, Any]]:
    cur = {str(r.get(key)): r for r in current}
    pri = {str(r.get(key)): r for r in prior}
    keys = set(cur) | set(pri)
    out: List[Dict[str, Any]] = []
    for k in keys:
        c = cur.get(k) or {}
        p = pri.get(k) or {}
        c_val = _as_float(c.get(value_key))
        p_val = _as_float(p.get(value_key))
        label = None
        if label_key:
            label = c.get(label_key) or p.get(label_key)
        if not label:
            label = str(k)
        out.append(
            {
                "key": k,
                "label": str(label),
                "current": c_val,
                "prior": p_val,
                "delta": round(c_val - p_val, 2),
            }
        )
    out.sort(key=lambda x: abs(x["delta"]), reverse=True)
    return out


def _contribution_factors(
    dimension: str,
    rows: List[Dict[str, Any]],
    *,
    total_delta: float,
) -> List[Dict[str, Any]]:
    factors: List[Dict[str, Any]] = []
    for row in rows[:TOP_FACTORS]:
        delta = float(row["delta"])
        if abs(delta) < 0.01:
            continue
        share = None
        if abs(total_delta) >= 0.01:
            share = round((delta / total_delta) * 100.0, 1)
        direction = "queda" if delta < 0 else "alta"
        factors.append(
            {
                "kind": "contribution",
                "dimension": dimension,
                "label": row["label"],
                "delta": delta,
                "current": row["current"],
                "prior": row["prior"],
                "share_of_total_delta_pct": share,
                "summary": (
                    f"{row['label']}: {direction} de R$ {abs(delta):,.2f} "
                    f"(atual R$ {row['current']:,.2f} vs base R$ {row['prior']:,.2f})"
                ),
                "evidence": {
                    "dimension": dimension,
                    "key": row["key"],
                    "source": _source_for_dimension(dimension),
                },
                # Contribuição observada ≠ causa comprovada.
                "causality": "not_proven",
            }
        )
    return factors


def _source_for_dimension(dimension: str) -> str:
    return {
        "filial": f"{MART_RT_DB}.sales_daily_rt",
        "grupo": f"{MART_RT_DB}.sales_groups_rt",
        "hora": f"{MART_RT_DB}.sales_hourly_rt",
    }.get(dimension, f"{MART_RT_DB}.sales_daily_rt")


def _hypotheses_from_playbook() -> List[Dict[str, Any]]:
    from app.intelligence.playbooks.engine import run_playbook

    pb = run_playbook("revenue_drop", context={}, can_view_profit=False) or {}
    out: List[Dict[str, Any]] = []
    for text in pb.get("hypotheses") or []:
        out.append(
            {
                "kind": "hypothesis",
                "label": str(text),
                "summary": str(text),
                "causality": "hypothesis",
                "evidence": {"source": "playbook:revenue_drop_v1"},
            }
        )
    return out


def _next_checks(pb_actions: List[Dict[str, Any]] | None = None) -> List[Dict[str, Any]]:
    from app.intelligence.playbooks.engine import run_playbook

    pb = run_playbook("revenue_drop", context={}, can_view_profit=False) or {}
    actions = pb_actions if pb_actions is not None else (pb.get("actions") or [])
    checks: List[Dict[str, Any]] = []
    for action in actions[:5]:
        checks.append(
            {
                "title": action.get("title"),
                "screen": action.get("screen") or "/sales",
                "suggested_owner": action.get("suggested_owner"),
                "deadline": action.get("deadline"),
            }
        )
    return checks


def investigate_sales_variation(
    role: str,
    id_empresa: int,
    id_filial: Any,
    dt_ini: date,
    dt_fim: date,
    *,
    as_of: Optional[date] = None,
    **_kwargs: Any,
) -> Dict[str, Any]:
    """Compara o período selecionado ao período anterior de mesma duração.

    Retorno sempre rotula comparação e limitações. Escopo vazio (`AND 0`)
    produz status ``forbidden_scope`` sem inventar totais.
    """
    del role  # role usado na rota para redaction; query já scoped por id_empresa/filial
    today = as_of or business_today()
    days = _period_days(dt_ini, dt_fim)
    if days > MAX_PERIOD_DAYS:
        return {
            "status": "period_too_long",
            "message": f"Período máximo para investigação: {MAX_PERIOD_DAYS} dias.",
            "max_period_days": MAX_PERIOD_DAYS,
            "period": {"dt_ini": dt_ini.isoformat(), "dt_fim": dt_fim.isoformat()},
        }

    # Escopo de filiais vazio autorizado → cláusula AND 0 (sem ampliar acesso).
    branch_clause = _branch_clause("id_filial", id_filial)
    if branch_clause.strip() == "AND 0":
        return {
            "status": "forbidden_scope",
            "message": "Nenhuma filial autorizada no escopo atual.",
            "period": {"dt_ini": dt_ini.isoformat(), "dt_fim": dt_fim.isoformat()},
        }

    prior_ini, prior_fim = prior_equal_period(dt_ini, dt_fim)
    period_incomplete = dt_fim >= today
    comparison = {
        "basis": "prior_equal_length",
        "basis_label": (
            "Período imediatamente anterior com a mesma quantidade de dias civis"
        ),
        "current": {
            "dt_ini": dt_ini.isoformat(),
            "dt_fim": dt_fim.isoformat(),
            "label": _period_label(dt_ini, dt_fim),
            "days": days,
        },
        "prior": {
            "dt_ini": prior_ini.isoformat(),
            "dt_fim": prior_fim.isoformat(),
            "label": _period_label(prior_ini, prior_fim),
            "days": _period_days(prior_ini, prior_fim),
        },
        "compatible": True,
        "period_incomplete": period_incomplete,
        "incomplete_note": (
            "O período atual ainda não terminou (inclui hoje ou data futura). "
            "Compare com cautela: a base anterior é um intervalo completo."
            if period_incomplete
            else None
        ),
    }

    try:
        current = _totals(id_empresa, id_filial, dt_ini, dt_fim)
        prior = _totals(id_empresa, id_filial, prior_ini, prior_fim)
        fil_cur = _by_filial(id_empresa, id_filial, dt_ini, dt_fim)
        fil_pri = _by_filial(id_empresa, id_filial, prior_ini, prior_fim)
        grp_cur = _by_group(id_empresa, id_filial, dt_ini, dt_fim)
        grp_pri = _by_group(id_empresa, id_filial, prior_ini, prior_fim)
        hr_cur = _by_hour(id_empresa, id_filial, dt_ini, dt_fim)
        hr_pri = _by_hour(id_empresa, id_filial, prior_ini, prior_fim)
    except Exception as exc:  # noqa: BLE001 — falha de leitura ≠ zero
        return {
            "status": "unavailable",
            "message": "Não foi possível consultar as marts de vendas agora.",
            "error": str(exc)[:200],
            "comparison": comparison,
            "totals": None,
            "factors": [],
            "next_checks": _next_checks(),
        }

    if not current["has_data"] and not prior["has_data"]:
        return {
            "status": "no_data",
            "message": (
                "Não há faturamento publicado na mart para o período nem para a base "
                "de comparação. Isso não é variação zero."
            ),
            "comparison": comparison,
            "totals": None,
            "factors": [],
            "next_checks": _next_checks(),
            "freshness": {"mode": "realtime", "source": f"{MART_RT_DB}.sales_daily_rt"},
        }

    warnings: List[str] = []
    if period_incomplete:
        warnings.append(comparison["incomplete_note"])
    if current["has_data"] and not prior["has_data"]:
        warnings.append(
            "A base de comparação não tem dados publicados; a variação absoluta "
            "usa zero na base apenas como referência matemática, não como fato operacional."
        )
        comparison["compatible"] = False
        comparison["compatible_note"] = "Base de comparação sem dados na mart."
    if prior["has_data"] and not current["has_data"]:
        warnings.append(
            "O período selecionado não tem dados publicados na mart (não confundir com R$ 0)."
        )

    cur_fat = current["faturamento"] if current["has_data"] else None
    pri_fat = prior["faturamento"] if prior["has_data"] else None
    if cur_fat is None or pri_fat is None:
        delta = None
        delta_pct = None
    else:
        delta = round(cur_fat - pri_fat, 2)
        delta_pct = (
            round((delta / pri_fat) * 100.0, 2) if abs(pri_fat) >= 0.01 else None
        )

    totals = {
        "current_faturamento": cur_fat,
        "prior_faturamento": pri_fat,
        "delta": delta,
        "delta_pct": delta_pct,
        "current_qtd_vendas": current["qtd_vendas"] if current["has_data"] else None,
        "prior_qtd_vendas": prior["qtd_vendas"] if prior["has_data"] else None,
        "current_has_data": current["has_data"],
        "prior_has_data": prior["has_data"],
    }

    factors: List[Dict[str, Any]] = []
    if delta is not None:
        fil_delta = _delta_map(
            fil_cur,
            fil_pri,
            key="id_filial",
            label_key=None,
        )
        for row in fil_delta:
            row["label"] = f"Filial {row['key']}"
        factors.extend(_contribution_factors("filial", fil_delta, total_delta=delta))

        grp_delta = _delta_map(
            grp_cur,
            grp_pri,
            key="id_grupo_produto",
            label_key="grupo_nome",
        )
        factors.extend(_contribution_factors("grupo", grp_delta, total_delta=delta))

        hr_delta = _delta_map(hr_cur, hr_pri, key="hora", label_key=None)
        for row in hr_delta:
            try:
                h = int(row["key"])
                row["label"] = f"{h:02d}h"
            except (TypeError, ValueError):
                row["label"] = f"Hora {row['key']}"
        factors.extend(_contribution_factors("hora", hr_delta, total_delta=delta))

        # Ordena contribuições por |delta| e limita; hipóteses depois.
        contrib = [f for f in factors if f.get("kind") == "contribution"]
        contrib.sort(key=lambda x: abs(float(x.get("delta") or 0)), reverse=True)
        factors = contrib[:12] + _hypotheses_from_playbook()
    else:
        factors = _hypotheses_from_playbook()

    last_updated = current.get("last_updated") or prior.get("last_updated")
    if isinstance(last_updated, datetime):
        freshness_ts = last_updated.astimezone(timezone.utc).isoformat()
    else:
        freshness_ts = str(last_updated) if last_updated else None

    headline = _headline(totals, comparison)

    return {
        "status": "ok",
        "headline": headline,
        "comparison": comparison,
        "totals": totals,
        "factors": factors,
        "next_checks": _next_checks(),
        "warnings": [w for w in warnings if w],
        "freshness": {
            "mode": "realtime",
            "source": f"{MART_RT_DB}.sales_daily_rt",
            "last_updated": freshness_ts,
        },
        "legend": {
            "contribution": (
                "Contribuição quantitativa observada na decomposição "
                "(não prova causa)."
            ),
            "hypothesis": "Hipótese operacional sugerida para verificação.",
            "proven_cause": "Não emitido nesta jornada — dados insuficientes para causalidade.",
        },
    }


def _headline(totals: Dict[str, Any], comparison: Dict[str, Any]) -> str:
    cur = totals.get("current_faturamento")
    pri = totals.get("prior_faturamento")
    delta = totals.get("delta")
    pct = totals.get("delta_pct")
    cur_label = comparison["current"]["label"]
    pri_label = comparison["prior"]["label"]
    if cur is None:
        return (
            f"Sem faturamento publicado em {cur_label}. "
            f"Base de comparação: {pri_label}."
        )
    if pri is None or delta is None:
        return (
            f"Faturamento em {cur_label}: R$ {cur:,.2f}. "
            f"Base {pri_label} indisponível para variação."
        )
    verb = "caiu" if delta < 0 else ("subiu" if delta > 0 else "ficou estável")
    pct_txt = f" ({pct:+.1f}%)" if pct is not None else ""
    return (
        f"Faturamento {verb} R$ {abs(delta):,.2f}{pct_txt}: "
        f"R$ {cur:,.2f} em {cur_label} vs R$ {pri:,.2f} em {pri_label} "
        f"({comparison['basis_label']})."
    )
