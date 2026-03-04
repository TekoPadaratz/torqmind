from __future__ import annotations

import hashlib
import json
import time
from datetime import date
from typing import Any, Dict, List, Optional

import httpx

from app.config import settings
from app.db import get_conn
from app.services.telegram import send_telegram_alert


def _estimate_cost_usd(prompt_tokens: int, completion_tokens: int) -> float:
    in_cost = (float(prompt_tokens or 0) / 1_000_000.0) * float(settings.jarvis_ai_input_cost_per_1m)
    out_cost = (float(completion_tokens or 0) / 1_000_000.0) * float(settings.jarvis_ai_output_cost_per_1m)
    return round(in_cost + out_cost, 8)


def _json_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "priority": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]},
            "diagnosis": {"type": "string"},
            "probable_causes": {"type": "array", "items": {"type": "string"}},
            "actions_today": {"type": "array", "items": {"type": "string"}},
            "actions_7d": {"type": "array", "items": {"type": "string"}},
            "validation_steps": {"type": "array", "items": {"type": "string"}},
            "expected_impact_range": {"type": "string"},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "data_gaps": {"type": "array", "items": {"type": "string"}},
        },
        "required": [
            "priority",
            "diagnosis",
            "probable_causes",
            "actions_today",
            "actions_7d",
            "validation_steps",
            "expected_impact_range",
            "confidence",
            "data_gaps",
        ],
    }


def _fallback_plan(insight: Dict[str, Any], error: Optional[str] = None) -> Dict[str, Any]:
    severity = str(insight.get("severity") or "WARN").upper()
    rec = str(insight.get("recommendation") or "Validar evidencias e agir no mesmo dia.")
    msg = str(insight.get("message") or "Risco operacional detectado.")
    impact = float(insight.get("impacto_estimado") or 0)

    plan = {
        "priority": "CRITICAL" if severity == "CRITICAL" else ("HIGH" if severity == "WARN" else "MEDIUM"),
        "diagnosis": msg,
        "probable_causes": [
            "Desvio operacional no turno/funcionario",
            "Processo sem dupla checagem no fechamento",
        ],
        "actions_today": [
            rec,
            "Investigar top 5 eventos por impacto e registrar acao corretiva",
        ],
        "actions_7d": [
            "Treinar equipe com foco no padrao do evento",
            "Ajustar regra/limite operacional e monitorar recorrencia",
        ],
        "validation_steps": [
            "Comparar incidencia antes vs depois da acao",
            "Medir impacto financeiro recuperado em 7 dias",
        ],
        "expected_impact_range": f"R$ {max(0.0, impact * 0.4):,.2f} a R$ {max(0.0, impact):,.2f}",
        "confidence": 0.55,
        "data_gaps": [] if impact > 0 else ["Sem impacto financeiro estimado para o insight"],
    }
    if error:
        plan["data_gaps"].append(f"Fallback deterministico ativado: {error}")
    return plan


def _parse_response_text(payload: Dict[str, Any]) -> str:
    text = payload.get("output_text")
    if isinstance(text, str) and text.strip():
        return text

    output = payload.get("output")
    if isinstance(output, list):
        for item in output:
            content = item.get("content") if isinstance(item, dict) else None
            if not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, dict):
                    continue
                if isinstance(block.get("text"), str) and block.get("text").strip():
                    return block["text"]
                if isinstance(block.get("json"), (dict, list)):
                    return json.dumps(block["json"], ensure_ascii=False)
    return ""


def _call_openai_structured(insight: Dict[str, Any], model: str) -> tuple[Dict[str, Any], int, int]:
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY not configured")

    evidence = {
        "insight_type": insight.get("insight_type"),
        "severity": insight.get("severity"),
        "impacto_estimado": float(insight.get("impacto_estimado") or 0),
        "title": insight.get("title"),
        "message": insight.get("message"),
        "recommendation": insight.get("recommendation"),
        "meta": insight.get("meta") or {},
    }

    payload = {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Voce e Auditor de Postos. Responda estritamente em JSON valido seguindo o schema, curto e acionavel.",
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(evidence, ensure_ascii=False),
                    }
                ],
            },
        ],
        "max_output_tokens": int(settings.jarvis_ai_max_output_tokens),
        "text": {
            "format": {
                "type": "json_schema",
                "name": "jarvis_plan",
                "strict": True,
                "schema": _json_schema(),
            }
        },
    }

    headers = {
        "Authorization": f"Bearer {settings.openai_api_key}",
        "Content-Type": "application/json",
    }

    retries = 3
    wait_base = float(settings.jarvis_ai_rpm_sleep_seconds)
    timeout = float(settings.jarvis_ai_timeout_seconds)

    with httpx.Client(timeout=timeout) as client:
        for attempt in range(1, retries + 1):
            resp = client.post("https://api.openai.com/v1/responses", headers=headers, json=payload)
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else (wait_base * attempt)
                time.sleep(wait)
                if attempt < retries:
                    continue
            resp.raise_for_status()
            body = resp.json()
            text = _parse_response_text(body)
            if not text:
                raise RuntimeError("Empty structured response from OpenAI")
            parsed = json.loads(text)
            usage = body.get("usage") if isinstance(body.get("usage"), dict) else {}
            prompt_tokens = int(usage.get("input_tokens") or 0)
            completion_tokens = int(usage.get("output_tokens") or 0)
            return parsed, prompt_tokens, completion_tokens

    raise RuntimeError("OpenAI call failed after retries")


def _candidate_insights(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ref: date,
    limit: int,
) -> List[Dict[str, Any]]:
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params: List[Any] = [id_empresa, dt_ref] + ([] if id_filial is None else [id_filial]) + [limit]

    sql = f"""
      SELECT
        id,
        id_empresa,
        id_filial,
        insight_type,
        severity,
        dt_ref,
        impacto_estimado,
        title,
        message,
        recommendation,
        meta,
        ai_plan,
        ai_generated_at
      FROM app.insights_gerados
      WHERE id_empresa = %s
        AND dt_ref = %s
        {where_filial}
      ORDER BY
        CASE severity WHEN 'CRITICAL' THEN 3 WHEN 'WARN' THEN 2 ELSE 1 END DESC,
        impacto_estimado DESC,
        id DESC
      LIMIT %s
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return list(conn.execute(sql, params).fetchall())


def _hash_for_insight(insight: Dict[str, Any], model: str) -> str:
    base = {
        "id_empresa": insight.get("id_empresa"),
        "id_filial": insight.get("id_filial"),
        "insight_type": insight.get("insight_type"),
        "dt_ref": str(insight.get("dt_ref")),
        "severity": insight.get("severity"),
        "impacto_estimado": float(insight.get("impacto_estimado") or 0),
        "title": insight.get("title"),
        "message": insight.get("message"),
        "recommendation": insight.get("recommendation"),
        "meta": insight.get("meta") or {},
        "model": model,
    }
    return hashlib.sha256(json.dumps(base, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def _read_cache(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    insight_hash: str,
    model: str,
) -> Optional[Dict[str, Any]]:
    where_filial = "id_filial IS NULL" if id_filial is None else "id_filial = %s"
    params = [id_empresa] + ([] if id_filial is None else [id_filial]) + [insight_hash, model]
    sql = f"""
      SELECT response_json, prompt_tokens, completion_tokens, estimated_cost_usd, source, error
      FROM app.insight_ai_cache
      WHERE id_empresa = %s
        AND {where_filial}
        AND insight_hash = %s
        AND model = %s
      ORDER BY created_at DESC
      LIMIT 1
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        return conn.execute(sql, params).fetchone()


def _write_cache(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    insight_hash: str,
    model: str,
    response_json: Dict[str, Any],
    prompt_tokens: int,
    completion_tokens: int,
    source: str,
    error: Optional[str],
) -> None:
    cost = _estimate_cost_usd(prompt_tokens, completion_tokens)
    sql = """
      INSERT INTO app.insight_ai_cache (
        id_empresa, id_filial, insight_hash, model, response_json,
        prompt_tokens, completion_tokens, estimated_cost_usd, source, error
      )
      VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s)
      ON CONFLICT ON CONSTRAINT uq_insight_ai_cache
      DO UPDATE SET
        response_json = EXCLUDED.response_json,
        prompt_tokens = EXCLUDED.prompt_tokens,
        completion_tokens = EXCLUDED.completion_tokens,
        estimated_cost_usd = EXCLUDED.estimated_cost_usd,
        source = EXCLUDED.source,
        error = EXCLUDED.error,
        created_at = now()
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        conn.execute(
            sql,
            (
                id_empresa,
                id_filial,
                insight_hash,
                model,
                json.dumps(response_json, ensure_ascii=False),
                int(prompt_tokens or 0),
                int(completion_tokens or 0),
                cost,
                source,
                error,
            ),
        )
        conn.commit()


def _attach_plan_to_insight(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    insight_id: int,
    model: str,
    plan: Dict[str, Any],
    prompt_tokens: int,
    completion_tokens: int,
    cache_hit: bool,
    error: Optional[str],
) -> None:
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params = [
        json.dumps(plan, ensure_ascii=False),
        model,
        int(prompt_tokens or 0),
        int(completion_tokens or 0),
        bool(cache_hit),
        error,
        id_empresa,
        insight_id,
    ] + ([] if id_filial is None else [id_filial])
    sql = f"""
      UPDATE app.insights_gerados
      SET
        ai_plan = %s::jsonb,
        ai_model = %s,
        ai_prompt_tokens = %s,
        ai_completion_tokens = %s,
        ai_generated_at = now(),
        ai_cache_hit = %s,
        ai_error = %s
      WHERE id_empresa = %s
        AND id = %s
        {where_filial}
    """
    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        conn.execute(sql, params)
        conn.commit()


def _create_notification_for_critical(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    insight: Dict[str, Any],
) -> None:
    if str(insight.get("severity") or "").upper() != "CRITICAL":
        return

    insight_type = str(insight.get("insight_type") or "")
    if "CHURN" in insight_type:
        path = "/customers"
    elif "CANCEL" in insight_type or "DESCONTO" in insight_type:
        path = "/fraud"
    else:
        path = "/finance"

    sql_update = """
      UPDATE app.notifications
      SET
        severity = %s,
        title = %s,
        body = %s,
        url = %s,
        created_at = now(),
        read_at = NULL
      WHERE id_empresa = %s
        AND insight_id = %s
        AND (
          (id_filial IS NULL AND %s IS NULL)
          OR id_filial = %s
        )
    """

    sql_insert = """
      INSERT INTO app.notifications (
        id_empresa, id_filial, insight_id, severity, title, body, url
      )
      VALUES (%s,%s,%s,%s,%s,%s,%s)
    """

    sev = "CRITICAL"
    title = str(insight.get("title") or "Alerta critico")
    body = str(insight.get("message") or "Risco critico detectado")
    ins_id = int(insight["id"])

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        cur = conn.execute(
            sql_update,
            (sev, title, body, path, id_empresa, ins_id, id_filial, id_filial),
        )
        if (cur.rowcount or 0) == 0:
            conn.execute(
                sql_insert,
                (id_empresa, id_filial, ins_id, sev, title, body, path),
            )
        conn.commit()

    send_telegram_alert(
        id_empresa=id_empresa,
        payload={
            "severity": "CRITICAL",
            "insight_id": ins_id,
            "insight_type": insight.get("insight_type"),
            "id_filial": id_filial,
            "event_time": str(insight.get("dt_ref") or ""),
            "impacto_estimado": float(insight.get("impacto_estimado") or 0),
            "title": title,
            "body": body,
            "url": path,
        },
    )


def generate_jarvis_ai_plans(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    dt_ref: date,
    limit: Optional[int] = None,
    force: bool = False,
) -> Dict[str, Any]:
    max_n = int(limit or settings.jarvis_ai_top_n)
    model = settings.jarvis_model_fast

    insights = _candidate_insights(role, id_empresa, id_filial, dt_ref, max_n)
    stats = {
        "requested": max_n,
        "candidates": len(insights),
        "processed": 0,
        "cache_hits": 0,
        "openai_calls": 0,
        "fallback_used": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "estimated_cost_usd": 0.0,
    }

    for ins in insights:
        if ins.get("ai_plan") and not force:
            continue

        insight_filial = int(ins["id_filial"]) if ins.get("id_filial") is not None else id_filial

        insight_hash = _hash_for_insight(ins, model)
        cached = _read_cache(role, id_empresa, insight_filial, insight_hash, model)

        plan: Dict[str, Any]
        prompt_tokens = 0
        completion_tokens = 0
        cache_hit = False
        source = "openai"
        error: Optional[str] = None

        if cached:
            plan = dict(cached.get("response_json") or {})
            prompt_tokens = int(cached.get("prompt_tokens") or 0)
            completion_tokens = int(cached.get("completion_tokens") or 0)
            cache_hit = True
            source = str(cached.get("source") or "openai")
            error = cached.get("error")
            stats["cache_hits"] += 1
        else:
            try:
                plan, prompt_tokens, completion_tokens = _call_openai_structured(ins, model)
                stats["openai_calls"] += 1
            except Exception as exc:  # noqa: BLE001
                source = "deterministic"
                error = str(exc)
                plan = _fallback_plan(ins, error=error)
                stats["fallback_used"] += 1

            _write_cache(
                role,
                id_empresa,
                insight_filial,
                insight_hash,
                model,
                response_json=plan,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                source=source,
                error=error,
            )

        _attach_plan_to_insight(
            role,
            id_empresa,
            insight_filial,
            insight_id=int(ins["id"]),
            model=model,
            plan=plan,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            cache_hit=cache_hit,
            error=error,
        )
        _create_notification_for_critical(role, id_empresa, insight_filial, ins)

        stats["processed"] += 1
        stats["prompt_tokens"] += prompt_tokens
        stats["completion_tokens"] += completion_tokens

    stats["estimated_cost_usd"] = _estimate_cost_usd(stats["prompt_tokens"], stats["completion_tokens"])
    return stats


def ai_usage_summary(
    role: str,
    id_empresa: int,
    id_filial: Optional[int],
    days: int,
) -> Dict[str, Any]:
    where_filial = "" if id_filial is None else "AND id_filial = %s"
    params: List[Any] = [id_empresa, max(1, int(days))] + ([] if id_filial is None else [id_filial])

    sql_total = f"""
      SELECT
        COALESCE(COUNT(*),0)::int AS cache_rows,
        COALESCE(SUM(prompt_tokens),0)::int AS prompt_tokens,
        COALESCE(SUM(completion_tokens),0)::int AS completion_tokens,
        COALESCE(SUM(estimated_cost_usd),0)::numeric(18,8) AS estimated_cost_usd,
        COALESCE(SUM(CASE WHEN source='openai' THEN 1 ELSE 0 END),0)::int AS openai_calls,
        COALESCE(SUM(CASE WHEN source='deterministic' THEN 1 ELSE 0 END),0)::int AS fallback_calls
      FROM app.insight_ai_cache
      WHERE id_empresa = %s
        AND created_at >= now() - make_interval(days => %s)
        {where_filial}
    """

    sql_by_model = f"""
      SELECT
        model,
        COALESCE(COUNT(*),0)::int AS calls,
        COALESCE(SUM(prompt_tokens),0)::int AS prompt_tokens,
        COALESCE(SUM(completion_tokens),0)::int AS completion_tokens,
        COALESCE(SUM(estimated_cost_usd),0)::numeric(18,8) AS estimated_cost_usd
      FROM app.insight_ai_cache
      WHERE id_empresa = %s
        AND created_at >= now() - make_interval(days => %s)
        {where_filial}
      GROUP BY model
      ORDER BY estimated_cost_usd DESC, calls DESC
    """

    sql_daily = f"""
      SELECT
        created_at::date AS dt,
        COALESCE(COUNT(*),0)::int AS calls,
        COALESCE(SUM(prompt_tokens),0)::int AS prompt_tokens,
        COALESCE(SUM(completion_tokens),0)::int AS completion_tokens,
        COALESCE(SUM(estimated_cost_usd),0)::numeric(18,8) AS estimated_cost_usd
      FROM app.insight_ai_cache
      WHERE id_empresa = %s
        AND created_at >= now() - make_interval(days => %s)
        {where_filial}
      GROUP BY created_at::date
      ORDER BY dt DESC
    """

    with get_conn(role=role, tenant_id=id_empresa, branch_id=id_filial) as conn:
        total = conn.execute(sql_total, params).fetchone() or {}
        by_model = list(conn.execute(sql_by_model, params).fetchall())
        daily = list(conn.execute(sql_daily, params).fetchall())

    return {
        "window_days": max(1, int(days)),
        "totals": total,
        "by_model": by_model,
        "daily": daily,
    }
