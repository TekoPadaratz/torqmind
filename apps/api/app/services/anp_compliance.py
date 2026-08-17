"""Compliance ANP — variação de margem em reajuste de PPL (bomba).

Fonte canônica (após backfill Agent/Xpert):
  stg.preco_bomba_hist  ← LMC ⨝ LMCBICOS (PPL diário)
  stg.itens_nfe_entrada ← COMPENTRADAS + ITENSCOMPROVANTE (custo compra)
  Evento = aumento de PPL médio do produto/dia ≥ R$ 0,03 × custo ASOF da entrada

Fallback legado: sales_products_rt (média de venda) se STG vazio.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.db import get_conn
from app.db_clickhouse import insert_batch, query_dict
from app.filial_apelido import apelido_for, set_apelido_scope

logger = logging.getLogger(__name__)

ORIGEM_LMC = "lmc_asof_entrada"
ORIGEM_LMC_SEM_NFE = "lmc_sem_nfe"
ORIGEM_VENDAS_DIA = "sales_products_rt"
ORIGEM_NFE = "nfe_asof"
# Mash mensal antigo sem lastro NFe — não serve o grid (Doc. entrada / Chave).
ORIGEM_MASH_MENSAL = "profit_produto_mensal"
ANP_MART_ORIGENS = (ORIGEM_LMC, ORIGEM_LMC_SEM_NFE, ORIGEM_NFE)
_ANP_MART_ORIGENS_SQL = ", ".join(f"'{o}'" for o in ANP_MART_ORIGENS)
EPS_MARGEM = 1e-6
PRECO_DELTA_MIN = 0.03
DEFAULT_ALERTA = 50.0
DEFAULT_ABUSIVO = 70.0
LOOKBACK_DAYS = 45


def classify_status(
    variacao_pct: Optional[float],
    limite_alerta: float,
    limite_abusivo: float,
    *,
    sem_lastro: bool = False,
) -> str:
    if sem_lastro or variacao_pct is None:
        return "SEM_LASTRO"
    if variacao_pct >= limite_abusivo:
        return "RISCO_ABUSIVO"
    if variacao_pct >= limite_alerta:
        return "ALERTA"
    return "OK"


def compute_variacao(
    preco_ant: float,
    custo_ant: float,
    preco_novo: float,
    custo_novo: float,
) -> Tuple[float, float, Optional[float], bool]:
    margem_ant = float(preco_ant) - float(custo_ant)
    margem_nova = float(preco_novo) - float(custo_novo)
    if abs(margem_ant) < EPS_MARGEM:
        return margem_ant, margem_nova, None, True
    variacao = ((margem_nova - margem_ant) / margem_ant) * 100.0
    return margem_ant, margem_nova, variacao, False


def mart_events_usable(events: Sequence[Dict[str, Any]]) -> bool:
    """Mart só alimenta o grid se trouxer número e/ou chave de NFe de entrada.

    `profit_produto_mensal` (mash antigo) nunca tem lastro fiscal; servir isso
    esconde Doc. entrada / Chave NFe. Eventos LMC/NFe sem chave também forçam
    recálculo live — a STG pode já ter a nota que a mart mash não publicou.
    """
    if not events:
        return False
    origens = {str(e.get("origem") or "") for e in events}
    if origens and origens <= {ORIGEM_MASH_MENSAL, ORIGEM_VENDAS_DIA}:
        return False
    return any(
        str(e.get("chave_nfe_nova") or e.get("numero_nota_nova") or "").strip()
        for e in events
    )


def load_config(id_empresa: int, id_filial: Optional[int] = None) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            if id_filial is not None and int(id_filial) > 0:
                cur.execute(
                    """
                    SELECT id_empresa, id_filial,
                           limite_alerta_amarelo_perc, limite_abusivo_anp_perc,
                           ativo, updated_at, updated_by
                      FROM app.anp_compliance_config
                     WHERE id_empresa = %s AND id_filial = %s AND ativo
                    """,
                    (id_empresa, int(id_filial)),
                )
                row = cur.fetchone()
                if row:
                    return dict(row)
            cur.execute(
                """
                SELECT id_empresa, id_filial,
                       limite_alerta_amarelo_perc, limite_abusivo_anp_perc,
                       ativo, updated_at, updated_by
                  FROM app.anp_compliance_config
                 WHERE id_empresa = %s AND id_filial = 0 AND ativo
                """,
                (id_empresa,),
            )
            row = cur.fetchone()
            if row:
                return dict(row)
    return {
        "id_empresa": id_empresa,
        "id_filial": 0,
        "limite_alerta_amarelo_perc": DEFAULT_ALERTA,
        "limite_abusivo_anp_perc": DEFAULT_ABUSIVO,
        "ativo": True,
        "updated_at": None,
        "updated_by": None,
    }


def upsert_config(
    id_empresa: int,
    id_filial: int,
    limite_alerta: float,
    limite_abusivo: float,
    updated_by: Optional[str] = None,
) -> Dict[str, Any]:
    if limite_alerta < 0 or limite_abusivo < limite_alerta:
        raise ValueError("Limites inválidos: alerta >= 0 e abusivo >= alerta.")
    fid = int(id_filial) if id_filial is not None else 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app.anp_compliance_config (
                  id_empresa, id_filial,
                  limite_alerta_amarelo_perc, limite_abusivo_anp_perc,
                  ativo, updated_at, updated_by
                ) VALUES (%s, %s, %s, %s, true, now(), %s)
                ON CONFLICT (id_empresa, id_filial) DO UPDATE SET
                  limite_alerta_amarelo_perc = EXCLUDED.limite_alerta_amarelo_perc,
                  limite_abusivo_anp_perc = EXCLUDED.limite_abusivo_anp_perc,
                  ativo = true,
                  updated_at = now(),
                  updated_by = EXCLUDED.updated_by
                RETURNING id_empresa, id_filial,
                          limite_alerta_amarelo_perc, limite_abusivo_anp_perc,
                          ativo, updated_at, updated_by
                """,
                (id_empresa, fid, limite_alerta, limite_abusivo, updated_by),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row)


def _configs_by_branch(id_empresa: int) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id_empresa, id_filial,
                       limite_alerta_amarelo_perc, limite_abusivo_anp_perc,
                       ativo, updated_at, updated_by
                  FROM app.anp_compliance_config
                 WHERE id_empresa = %s AND ativo
                """,
                (id_empresa,),
            )
            for row in cur.fetchall() or []:
                out[int(row["id_filial"])] = dict(row)
    if 0 not in out:
        out[0] = {
            "id_empresa": id_empresa,
            "id_filial": 0,
            "limite_alerta_amarelo_perc": DEFAULT_ALERTA,
            "limite_abusivo_anp_perc": DEFAULT_ABUSIVO,
            "ativo": True,
        }
    return out


def _limits_for(cfg_map: Dict[int, Dict[str, Any]], id_filial: int) -> Tuple[float, float]:
    cfg = cfg_map.get(int(id_filial)) or cfg_map.get(0) or {}
    return (
        float(cfg.get("limite_alerta_amarelo_perc") or DEFAULT_ALERTA),
        float(cfg.get("limite_abusivo_anp_perc") or DEFAULT_ABUSIVO),
    )


def _parse_date(value: Any) -> Optional[date]:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _stg_has_lmc(id_empresa: int, branch_ids: Sequence[int]) -> bool:
    if not branch_ids:
        return False
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                  FROM stg.preco_bomba_hist
                 WHERE id_empresa = %s
                   AND id_filial = ANY(%s)
                   AND preco_venda_shadow > 0
                 LIMIT 1
                """,
                (id_empresa, list(branch_ids)),
            )
            return cur.fetchone() is not None


def compute_lmc_events(
    id_empresa: int,
    branch_ids: Sequence[int],
    dt_ini: date,
    dt_fim: date,
) -> List[Dict[str, Any]]:
    """Eventos de aumento de PPL (LMC) × custo ASOF da entrada (COMPENTRADAS)."""
    if not branch_ids:
        return []
    set_apelido_scope(id_empresa)
    cfg_map = _configs_by_branch(id_empresa)
    lookback_from = dt_ini - timedelta(days=LOOKBACK_DAYS)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id_filial, id_produto,
                       COALESCE(NULLIF(btrim(payload->>'NOMEPRODUTO'), ''), '') AS nome_produto,
                       date_trunc('day', dt_alteracao_shadow AT TIME ZONE 'America/Sao_Paulo') AS dia,
                       AVG(preco_venda_shadow)::float8 AS ppl
                  FROM stg.preco_bomba_hist
                 WHERE id_empresa = %s
                   AND id_filial = ANY(%s)
                   AND preco_venda_shadow > 0
                   AND (dt_alteracao_shadow AT TIME ZONE 'America/Sao_Paulo')::date BETWEEN %s AND %s
                 GROUP BY 1, 2, 3, 4
                 ORDER BY 1, 2, 4
                """,
                (id_empresa, list(branch_ids), lookback_from, dt_fim),
            )
            series = [dict(r) for r in cur.fetchall() or []]

            cur.execute(
                """
                SELECT i.id_filial,
                       i.id_produto_shadow AS id_produto,
                       i.custo_unitario_shadow::float8 AS custo,
                       COALESCE(n.dt_entrada_shadow, i.dt_evento) AS dt_entrada,
                       COALESCE(n.chave_acesso_shadow, '') AS chave,
                       COALESCE(n.numero_nota_shadow, '') AS numero,
                       COALESCE(n.cnpj_emitente_shadow, '') AS cnpj
                  FROM stg.itens_nfe_entrada i
                  JOIN stg.nfe_entrada n
                    ON n.id_empresa = i.id_empresa
                   AND n.id_filial = i.id_filial
                   AND n.id_db = i.id_db
                   AND n.id_nota = i.id_nota
                 WHERE i.id_empresa = %s
                   AND i.id_filial = ANY(%s)
                   AND COALESCE(i.eh_combustivel_shadow, false)
                   AND i.custo_unitario_shadow > 0
                 ORDER BY i.id_filial, i.id_produto_shadow,
                          COALESCE(n.dt_entrada_shadow, i.dt_evento)
                """,
                (id_empresa, list(branch_ids)),
            )
            custos = [dict(r) for r in cur.fetchall() or []]

    def asof_custo(id_filial: int, id_produto: int, as_of: date) -> Optional[Dict[str, Any]]:
        best = None
        for c in custos:
            if int(c["id_filial"]) != id_filial or int(c.get("id_produto") or 0) != id_produto:
                continue
            d = _parse_date(c.get("dt_entrada"))
            if d is None or d > as_of:
                continue
            best = c
        return best

    by: Dict[Tuple[int, int], List[Dict[str, Any]]] = defaultdict(list)
    for r in series:
        by[(int(r["id_filial"]), int(r["id_produto"]))].append(r)

    now = datetime.now(timezone.utc)
    events: List[Dict[str, Any]] = []
    for (fil, prod), rows in by.items():
        rows = sorted(rows, key=lambda x: x["dia"] or datetime.min)
        alerta, abusivo = _limits_for(cfg_map, fil)
        nome_resumido = apelido_for(fil) or ""
        prev = None
        for curr in rows:
            if prev is None:
                prev = curr
                continue
            p_ant = float(prev["ppl"])
            p_novo = float(curr["ppl"])
            if p_novo < p_ant + PRECO_DELTA_MIN:
                prev = curr
                continue
            d_evt = _parse_date(curr.get("dia"))
            d_prev = _parse_date(prev.get("dia"))
            if d_evt is None or d_prev is None:
                prev = curr
                continue
            if d_evt < dt_ini or d_evt > dt_fim:
                prev = curr
                continue

            c_ant = asof_custo(fil, prod, d_prev)
            c_novo = asof_custo(fil, prod, d_evt)
            if not c_ant or not c_novo:
                margem_ant = margem_nova = 0.0
                variacao = None
                status = "SEM_LASTRO"
                custo_ant = custo_novo = 0.0
                chave = numero = cnpj = ""
                origem = ORIGEM_LMC_SEM_NFE
            else:
                custo_ant = float(c_ant["custo"])
                custo_novo = float(c_novo["custo"])
                margem_ant, margem_nova, variacao, sem = compute_variacao(
                    p_ant, custo_ant, p_novo, custo_novo
                )
                status = classify_status(variacao, alerta, abusivo, sem_lastro=sem)
                chave = str(c_novo.get("chave") or "")
                numero = str(c_novo.get("numero") or "")
                cnpj = str(c_novo.get("cnpj") or "")
                origem = ORIGEM_LMC

            events.append(
                {
                    "id_empresa": id_empresa,
                    "id_filial": fil,
                    "id_produto": prod,
                    "nome_resumido": nome_resumido,
                    "nome_produto": str(curr.get("nome_produto") or ""),
                    "dt_alteracao_preco": datetime(d_evt.year, d_evt.month, d_evt.day, 12, 0, 0),
                    "data_alteracao": d_evt.isoformat(),
                    "preco_venda_anterior": round(p_ant, 4),
                    "preco_venda_novo": round(p_novo, 4),
                    "custo_nfe_anterior": round(custo_ant, 6),
                    "custo_nfe_novo": round(custo_novo, 6),
                    "margem_anterior": round(margem_ant, 6),
                    "margem_nova": round(margem_nova, 6),
                    "variacao_margem_pct": None if variacao is None else round(variacao, 4),
                    "limite_alerta_perc": alerta,
                    "limite_abusivo_perc": abusivo,
                    "status": status,
                    "chave_nfe_anterior": "",
                    "chave_nfe_nova": chave,
                    "cnpj_emitente_nova": cnpj,
                    "numero_nota_nova": numero,
                    "dt_entrada_nfe_nova": None,
                    "origem": origem,
                    "published_at": now,
                }
            )
            prev = curr

    events.sort(
        key=lambda e: (
            str(e.get("nome_resumido") or ""),
            int(e.get("id_filial") or 0),
            str(e.get("data_alteracao") or ""),
            e.get("nome_produto") or "",
        )
    )
    return events


def _data_key(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def _branch_sql(branch_ids: Sequence[int], params: Dict[str, Any]) -> str:
    if len(branch_ids) == 1:
        params["id_filial"] = int(branch_ids[0])
        return "AND id_filial = %(id_filial)s"
    params["branch_ids"] = [int(b) for b in branch_ids]
    return "AND id_filial IN %(branch_ids)s"


def compute_daily_events(
    id_empresa: int,
    branch_ids: Sequence[int],
    dt_ini: date,
    dt_fim: date,
) -> List[Dict[str, Any]]:
    """Fallback: sales_products_rt (só se STG LMC vazio)."""
    if not branch_ids or dt_ini is None or dt_fim is None:
        return []
    if dt_fim < dt_ini:
        dt_ini, dt_fim = dt_fim, dt_ini

    set_apelido_scope(id_empresa)
    cfg_map = _configs_by_branch(id_empresa)
    lookback_from = dt_ini - timedelta(days=LOOKBACK_DAYS)
    params: Dict[str, Any] = {
        "id_empresa": int(id_empresa),
        "dk_from": _data_key(lookback_from),
        "dk_to": _data_key(dt_fim),
    }
    branch_sql = _branch_sql(branch_ids, params)
    rows = query_dict(
        f"""
        SELECT
            id_empresa, id_filial, id_produto,
            any(nome_produto) AS nome_produto,
            data_key,
            any(dt) AS dt,
            toFloat64(sum(faturamento)) / nullIf(sum(qtd), 0) AS preco,
            toFloat64(sum(custo_total)) / nullIf(sum(qtd), 0) AS custo
        FROM torqmind_mart_rt.sales_products_rt FINAL
        WHERE id_empresa = %(id_empresa)s
          {branch_sql}
          AND data_key BETWEEN %(dk_from)s AND %(dk_to)s
          AND upperUTF8(nome_grupo) LIKE '%%COMBUST%%'
          AND qtd > 0 AND faturamento > 0 AND custo_total > 0
        GROUP BY id_empresa, id_filial, id_produto, data_key
        ORDER BY id_empresa, id_filial, id_produto, data_key
        """,
        parameters=params,
        tenant_id=id_empresa,
    )

    by_key: Dict[Tuple[int, int, int], List[Dict[str, Any]]] = {}
    for r in rows:
        key = (int(r["id_empresa"]), int(r["id_filial"]), int(r["id_produto"]))
        by_key.setdefault(key, []).append(r)

    events: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    dk_ini, dk_fim = _data_key(dt_ini), _data_key(dt_fim)
    for (emp, fil, prod), series in by_key.items():
        series = sorted(series, key=lambda x: int(x["data_key"]))
        alerta, abusivo = _limits_for(cfg_map, fil)
        nome_resumido = apelido_for(fil) or ""
        prev = None
        for curr in series:
            if prev is None:
                prev = curr
                continue
            preco_ant = float(prev["preco"])
            preco_novo = float(curr["preco"])
            if preco_novo < preco_ant + PRECO_DELTA_MIN:
                prev = curr
                continue
            if int(curr["data_key"]) < dk_ini or int(curr["data_key"]) > dk_fim:
                prev = curr
                continue
            dt_evt = _parse_date(curr.get("dt")) or date(
                int(str(curr["data_key"])[:4]),
                int(str(curr["data_key"])[4:6]),
                int(str(curr["data_key"])[6:8]),
            )
            custo_ant = float(prev["custo"])
            custo_novo = float(curr["custo"])
            margem_ant, margem_nova, variacao, sem = compute_variacao(
                preco_ant, custo_ant, preco_novo, custo_novo
            )
            status = classify_status(variacao, alerta, abusivo, sem_lastro=sem)
            events.append(
                {
                    "id_empresa": emp,
                    "id_filial": fil,
                    "id_produto": prod,
                    "nome_resumido": nome_resumido,
                    "nome_produto": str(curr.get("nome_produto") or ""),
                    "dt_alteracao_preco": datetime(dt_evt.year, dt_evt.month, dt_evt.day, 12, 0, 0),
                    "data_alteracao": dt_evt.isoformat(),
                    "preco_venda_anterior": round(preco_ant, 4),
                    "preco_venda_novo": round(preco_novo, 4),
                    "custo_nfe_anterior": round(custo_ant, 6),
                    "custo_nfe_novo": round(custo_novo, 6),
                    "margem_anterior": round(margem_ant, 6),
                    "margem_nova": round(margem_nova, 6),
                    "variacao_margem_pct": None if variacao is None else round(variacao, 4),
                    "limite_alerta_perc": alerta,
                    "limite_abusivo_perc": abusivo,
                    "status": status,
                    "chave_nfe_anterior": "",
                    "chave_nfe_nova": "",
                    "cnpj_emitente_nova": "",
                    "numero_nota_nova": "",
                    "dt_entrada_nfe_nova": None,
                    "origem": ORIGEM_VENDAS_DIA,
                    "published_at": now,
                }
            )
            prev = curr
    events.sort(
        key=lambda e: (
            str(e.get("nome_resumido") or ""),
            int(e.get("id_filial") or 0),
            str(e.get("data_alteracao") or ""),
            e.get("nome_produto") or "",
        )
    )
    return events


def overview_payload(
    id_empresa: int,
    branch_ids: Sequence[int],
    dt_ini: Optional[date] = None,
    dt_fim: Optional[date] = None,
    *,
    prefer_mart: bool = True,
) -> Dict[str, Any]:
    set_apelido_scope(id_empresa)
    if dt_ini is None or dt_fim is None:
        today = date.today()
        dt_fim = dt_fim or today
        dt_ini = dt_ini or (dt_fim - timedelta(days=30))

    events: List[Dict[str, Any]] = []
    source = ORIGEM_LMC

    if prefer_mart:
        events = _events_from_ch_mart(id_empresa, branch_ids, dt_ini, dt_fim)
        if events and mart_events_usable(events):
            source = str(events[0].get("origem") or "clickhouse_mart")
        else:
            events = []
            source = ORIGEM_LMC

    if not events:
        if _stg_has_lmc(id_empresa, branch_ids):
            events = compute_lmc_events(id_empresa, branch_ids, dt_ini, dt_fim)
            if events:
                source = str(events[0].get("origem") or ORIGEM_LMC)
            else:
                source = ORIGEM_LMC
        if not events and not _stg_has_lmc(id_empresa, branch_ids):
            events = compute_daily_events(id_empresa, branch_ids, dt_ini, dt_fim)
            source = ORIGEM_VENDAS_DIA if events else ORIGEM_LMC

    for e in events:
        if not e.get("nome_resumido"):
            e["nome_resumido"] = apelido_for(int(e["id_filial"])) or ""
        if not e.get("data_alteracao") and e.get("dt_alteracao_preco"):
            parsed = _parse_date(e["dt_alteracao_preco"])
            if parsed:
                e["data_alteracao"] = parsed.isoformat()

    counts = {"OK": 0, "ALERTA": 0, "RISCO_ABUSIVO": 0, "SEM_LASTRO": 0}
    for e in events:
        st = str(e.get("status") or "OK")
        counts[st] = counts.get(st, 0) + 1

    cfg = load_config(id_empresa, branch_ids[0] if len(branch_ids) == 1 else 0)
    return {
        "origem": source,
        "periodo": {"dt_ini": dt_ini.isoformat(), "dt_fim": dt_fim.isoformat()},
        "filiais": [int(b) for b in branch_ids],
        "total_eventos": len(events),
        "contadores": counts,
        "config": {
            "limite_alerta_amarelo_perc": float(cfg["limite_alerta_amarelo_perc"]),
            "limite_abusivo_anp_perc": float(cfg["limite_abusivo_anp_perc"]),
            "id_filial_config": int(cfg.get("id_filial") or 0),
        },
        "eventos": events,
    }


def _events_from_ch_mart(
    id_empresa: int,
    branch_ids: Sequence[int],
    dt_ini: date,
    dt_fim: date,
) -> List[Dict[str, Any]]:
    """Leitura canônica: torqmind_mart_rt.mart_anp_compliance."""
    if not branch_ids:
        return []
    params: Dict[str, Any] = {
        "id_empresa": int(id_empresa),
        "dt_ini": dt_ini.isoformat(),
        "dt_fim": (dt_fim + timedelta(days=1)).isoformat(),
    }
    if len(branch_ids) == 1:
        branch_sql = "AND id_filial = %(id_filial)s"
        params["id_filial"] = int(branch_ids[0])
    else:
        ids = ", ".join(str(int(b)) for b in branch_ids)
        branch_sql = f"AND id_filial IN ({ids})"
    try:
        rows = query_dict(
            f"""
            SELECT
                id_empresa, id_filial, id_produto, nome_resumido, nome_produto,
                dt_alteracao_preco, preco_venda_anterior, preco_venda_novo,
                custo_nfe_anterior, custo_nfe_novo, margem_anterior, margem_nova,
                variacao_margem_pct, limite_alerta_perc, limite_abusivo_perc,
                status, chave_nfe_anterior, chave_nfe_nova, cnpj_emitente_nova,
                numero_nota_nova, dt_entrada_nfe_nova, origem, published_at
            FROM torqmind_mart_rt.mart_anp_compliance FINAL
            WHERE id_empresa = %(id_empresa)s
              {branch_sql}
              AND origem IN ({_ANP_MART_ORIGENS_SQL})
              AND dt_alteracao_preco >= toDateTime64(%(dt_ini)s, 3, 'America/Sao_Paulo')
              AND dt_alteracao_preco < toDateTime64(%(dt_fim)s, 3, 'America/Sao_Paulo')
            ORDER BY dt_alteracao_preco DESC, id_filial, id_produto
            LIMIT 5000
            """,
            params,
        )
    except Exception as exc:
        logger.warning("ANP CH mart read failed: %s", str(exc)[:200])
        return []

    events: List[Dict[str, Any]] = []
    for r in rows:
        dt = r.get("dt_alteracao_preco")
        events.append({
            **{k: r.get(k) for k in (
                "id_empresa", "id_filial", "id_produto", "nome_resumido", "nome_produto",
                "preco_venda_anterior", "preco_venda_novo", "custo_nfe_anterior", "custo_nfe_novo",
                "margem_anterior", "margem_nova", "variacao_margem_pct",
                "chave_nfe_anterior", "chave_nfe_nova", "cnpj_emitente_nova",
                "numero_nota_nova", "dt_entrada_nfe_nova", "origem",
            )},
            "dt_alteracao_preco": dt.isoformat() if hasattr(dt, "isoformat") else dt,
            "limite_alerta_perc": r.get("limite_alerta_perc"),
            "limite_abusivo_perc": r.get("limite_abusivo_perc"),
            "status": r.get("status"),
            "data_alteracao": dt.date().isoformat() if hasattr(dt, "date") else None,
        })
    return events


def publish_proxy_to_mart(
    id_empresa: int,
    branch_ids: Sequence[int],
    dt_ini: Optional[date] = None,
    dt_fim: Optional[date] = None,
) -> Dict[str, Any]:
    if dt_ini is None or dt_fim is None:
        today = date.today()
        dt_fim = dt_fim or today
        dt_ini = dt_ini or (dt_fim - timedelta(days=30))
    if _stg_has_lmc(id_empresa, branch_ids):
        events = compute_lmc_events(id_empresa, branch_ids, dt_ini, dt_fim)
    else:
        events = compute_daily_events(id_empresa, branch_ids, dt_ini, dt_fim)
    if not events:
        return {"inserted": 0, "origem": ORIGEM_LMC}
    rows = []
    for e in events:
        rows.append(
            {
                k: e.get(k)
                for k in (
                    "id_empresa",
                    "id_filial",
                    "id_produto",
                    "nome_resumido",
                    "nome_produto",
                    "dt_alteracao_preco",
                    "preco_venda_anterior",
                    "preco_venda_novo",
                    "custo_nfe_anterior",
                    "custo_nfe_novo",
                    "margem_anterior",
                    "margem_nova",
                    "variacao_margem_pct",
                    "limite_alerta_perc",
                    "limite_abusivo_perc",
                    "status",
                    "chave_nfe_anterior",
                    "chave_nfe_nova",
                    "cnpj_emitente_nova",
                    "numero_nota_nova",
                    "dt_entrada_nfe_nova",
                    "origem",
                    "published_at",
                )
            }
        )
    inserted = insert_batch(
        "torqmind_mart_rt.mart_anp_compliance",
        rows,
        order_by=["id_empresa", "id_filial", "id_produto", "dt_alteracao_preco"],
    )
    return {"inserted": inserted, "origem": rows[0].get("origem") if rows else ORIGEM_LMC}


def events_to_csv(events: Sequence[Dict[str, Any]]) -> str:
    headers = [
        "nome_resumido",
        "id_filial",
        "nome_produto",
        "data_alteracao",
        "preco_venda_anterior",
        "preco_venda_novo",
        "custo_nfe_anterior",
        "custo_nfe_novo",
        "margem_anterior",
        "margem_nova",
        "variacao_margem_pct",
        "status",
        "chave_nfe_nova",
        "cnpj_emitente_nova",
        "numero_nota_nova",
    ]

    def esc(v: Any) -> str:
        s = "" if v is None else str(v)
        if any(c in s for c in (",", '"', "\n")):
            return '"' + s.replace('"', '""') + '"'
        return s

    lines = [",".join(headers)]
    for e in events:
        lines.append(",".join(esc(e.get(h)) for h in headers))
    return "\n".join(lines) + "\n"
