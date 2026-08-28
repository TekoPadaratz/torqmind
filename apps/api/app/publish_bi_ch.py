"""Publish BI hot-path marts PostgreSQL → ClickHouse.

Mash continua no PG; a API de tela lê exclusivamente as tabelas CH
criadas em ``sql/clickhouse/streaming/053_bi_hotpath_ch_marts.sql``.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.db import get_conn
from app.db_clickhouse import insert_batch

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _fetch(role: str, id_empresa: int, sql: str, params: list) -> List[Dict[str, Any]]:
    with get_conn(role=role, tenant_id=id_empresa, branch_id=None) as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def publish_despesa_conta_mensal(role: str, id_empresa: int) -> int:
    rows = _fetch(
        role,
        id_empresa,
        """
        SELECT d.id_empresa, d.id_filial, d.id_plano_conta, d.ano, d.mes,
               COALESCE(g.codigo, '') AS codigo,
               COALESCE(g.nome_conta, '') AS nome_conta,
               COALESCE(d.valor_realizado, 0) AS valor_realizado,
               COALESCE(d.qtd, 0) AS qtd
        FROM mart.despesa_conta_mensal d
        LEFT JOIN mart.plano_contas_gerencial g
          ON g.id_empresa = d.id_empresa AND g.id_filial = d.id_filial
         AND g.id_plano_conta = d.id_plano_conta
        WHERE d.id_empresa = %s
        """,
        [id_empresa],
    )
    pub = _now()
    # Insert por ano para respeitar max_partitions_per_insert_block
    by_ano: Dict[int, List[Dict[str, Any]]] = {}
    for r in rows:
        ano = int(r["ano"])
        by_ano.setdefault(ano, []).append({
            "id_empresa": int(r["id_empresa"]),
            "id_filial": int(r["id_filial"]),
            "id_plano_conta": int(r["id_plano_conta"]),
            "ano": ano,
            "mes": int(r["mes"]),
            "codigo": str(r.get("codigo") or ""),
            "nome_conta": str(r.get("nome_conta") or ""),
            "valor_realizado": float(r.get("valor_realizado") or 0),
            "qtd": int(r.get("qtd") or 0),
            "published_at": pub,
        })
    total = 0
    for ano in sorted(by_ano):
        total += insert_batch(
            "torqmind_mart_rt.mart_despesa_conta_mensal",
            by_ano[ano],
            order_by=["id_empresa", "id_filial", "ano", "mes", "id_plano_conta"],
        )
    return total


def publish_plano_contas_gerencial(role: str, id_empresa: int) -> int:
    rows = _fetch(
        role,
        id_empresa,
        """
        SELECT id_empresa, id_filial, id_plano_conta,
               COALESCE(codigo, '') AS codigo,
               COALESCE(nome_conta, '') AS nome_conta
        FROM mart.plano_contas_gerencial
        WHERE id_empresa = %s
        """,
        [id_empresa],
    )
    pub = _now()
    ch = [{
        "id_empresa": int(r["id_empresa"]),
        "id_filial": int(r["id_filial"]),
        "id_plano_conta": int(r["id_plano_conta"]),
        "codigo": str(r.get("codigo") or ""),
        "nome_conta": str(r.get("nome_conta") or ""),
        "published_at": pub,
    } for r in rows]
    return insert_batch(
        "torqmind_mart_rt.mart_plano_contas_gerencial",
        ch,
        order_by=["id_empresa", "id_filial", "id_plano_conta"],
    )


def publish_cheques_pendentes(role: str, id_empresa: int) -> int:
    rows = _fetch(
        role,
        id_empresa,
        """
        SELECT id_empresa, id_filial, id_db, id_cheque, id_entidade,
               cliente_nome, cpf, valor, dt_recebido, dt_vencimento, dt_compensado,
               situacao_cheque, avista, motivo_devolucao, status_cheque,
               banco, agencia, nroconta, numero
        FROM mart.cheques_pendentes
        WHERE id_empresa = %s
        """,
        [id_empresa],
    )
    pub = _now()
    ch = [{
        "id_empresa": int(r["id_empresa"]),
        "id_filial": int(r["id_filial"]),
        "id_db": int(r.get("id_db") or 0),
        "id_cheque": int(r["id_cheque"]),
        "id_entidade": int(r.get("id_entidade") or 0),
        "cliente_nome": str(r.get("cliente_nome") or ""),
        "cpf": str(r.get("cpf") or ""),
        "valor": float(r.get("valor") or 0),
        "dt_recebido": r.get("dt_recebido"),
        "dt_vencimento": r.get("dt_vencimento"),
        "dt_compensado": r.get("dt_compensado"),
        "situacao_cheque": int(r.get("situacao_cheque") or 0),
        "avista": 1 if r.get("avista") else 0,
        "motivo_devolucao": str(r.get("motivo_devolucao") or ""),
        "status_cheque": str(r.get("status_cheque") or "a_compensar"),
        "banco": str(r.get("banco") or ""),
        "agencia": str(r.get("agencia") or ""),
        "nroconta": str(r.get("nroconta") or ""),
        "numero": str(r.get("numero") or ""),
        "published_at": pub,
    } for r in rows]
    return insert_batch(
        "torqmind_mart_rt.mart_cheques_pendentes",
        ch,
        order_by=["id_empresa", "id_filial", "id_db", "id_cheque"],
    )


def publish_ticket_combustivel(role: str, id_empresa: int) -> int:
    rows = _fetch(
        role,
        id_empresa,
        """
        SELECT id_empresa, id_filial, data_ref, valor_total, litros_total,
               qtd_abastecimentos, ticket_medio
        FROM mart.ticket_combustivel_diaria
        WHERE id_empresa = %s
        """,
        [id_empresa],
    )
    pub = _now()
    ch = [{
        "id_empresa": int(r["id_empresa"]),
        "id_filial": int(r["id_filial"]),
        "data_ref": r["data_ref"],
        "valor_total": float(r.get("valor_total") or 0),
        "litros_total": float(r.get("litros_total") or 0),
        "qtd_abastecimentos": int(r.get("qtd_abastecimentos") or 0),
        "ticket_medio": float(r.get("ticket_medio") or 0),
        "published_at": pub,
    } for r in rows]
    return insert_batch(
        "torqmind_mart_rt.mart_ticket_combustivel_diaria",
        ch,
        order_by=["id_empresa", "id_filial", "data_ref"],
    )


def publish_solvencia_itens(role: str, id_empresa: int) -> Dict[str, int]:
    """Publica itens/bancos/liquidez/a-prazo → ClickHouse.

    O mash PG (``etl.refresh_liquidez_*`` / ``refresh_solvencia_itens``) roda no
    cron ``prod-pg-profit-refresh.sh`` *antes* deste publish — não reexecutar aqui.
    """
    pub = _now()
    items = _fetch(
        role,
        id_empresa,
        """
        SELECT id_empresa, id_filial, grupo, secao, item_label, valor, qtd, origem, ordem
        FROM mart.solvencia_item WHERE id_empresa = %s
        """,
        [id_empresa],
    )
    n_item = insert_batch(
        "torqmind_mart_rt.mart_solvencia_item",
        [{
            "id_empresa": int(r["id_empresa"]),
            "id_filial": int(r["id_filial"]),
            "grupo": str(r.get("grupo") or ""),
            "secao": str(r.get("secao") or ""),
            "item_label": str(r.get("item_label") or ""),
            "valor": float(r.get("valor") or 0),
            "qtd": float(r.get("qtd") or 0),
            "origem": str(r.get("origem") or "auto"),
            "ordem": int(r.get("ordem") or 0),
            "published_at": pub,
        } for r in items],
        order_by=["id_empresa", "id_filial", "grupo", "secao", "item_label"],
    )

    bancos = _fetch(
        role,
        id_empresa,
        """
        SELECT id_empresa, id_filial, ano_mes, id_contasbancarias, banco_nome,
               agencia, nro_conta, descricao, ativo, saldo
        FROM mart.solvencia_banco_conta WHERE id_empresa = %s
        """,
        [id_empresa],
    )
    n_banco = insert_batch(
        "torqmind_mart_rt.mart_solvencia_banco_conta",
        [{
            "id_empresa": int(r["id_empresa"]),
            "id_filial": int(r["id_filial"]),
            "ano_mes": int(r["ano_mes"]),
            "id_contasbancarias": int(r["id_contasbancarias"]),
            "banco_nome": str(r.get("banco_nome") or ""),
            "agencia": str(r.get("agencia") or ""),
            "nro_conta": str(r.get("nro_conta") or ""),
            "descricao": str(r.get("descricao") or ""),
            "ativo": 1 if r.get("ativo") else 0,
            "saldo": float(r.get("saldo") or 0),
            "published_at": pub,
        } for r in bancos],
        order_by=["id_empresa", "id_filial", "ano_mes", "id_contasbancarias"],
    )

    liq = _fetch(
        role,
        id_empresa,
        """
        SELECT id_empresa, id_filial, ano_mes,
               passivo_contas_pagar, passivo_qtd_titulos, passivo_vencido,
               ativo_caixa, ativo_banco, ativo_cartoes, ativo_cheques, ativo_estoque,
               ativo_estoque_combustivel, ativo_estoque_loja,
               COALESCE(ativo_cartoes_credito, 0) AS ativo_cartoes_credito,
               COALESCE(ativo_cartoes_debito, 0) AS ativo_cartoes_debito,
               tem_ativo_dados, estoque_combustivel_medido, estoque_data_leitura
        FROM mart.liquidez_solvencia WHERE id_empresa = %s
        """,
        [id_empresa],
    )
    n_liq = insert_batch(
        "torqmind_mart_rt.mart_liquidez_solvencia",
        [{
            "id_empresa": int(r["id_empresa"]),
            "id_filial": int(r["id_filial"]),
            "ano_mes": int(r["ano_mes"]),
            "passivo_contas_pagar": float(r.get("passivo_contas_pagar") or 0),
            "passivo_qtd_titulos": int(r.get("passivo_qtd_titulos") or 0),
            "passivo_vencido": float(r.get("passivo_vencido") or 0),
            "ativo_caixa": float(r.get("ativo_caixa") or 0),
            "ativo_banco": float(r.get("ativo_banco") or 0),
            "ativo_cartoes": float(r.get("ativo_cartoes") or 0),
            "ativo_cheques": float(r.get("ativo_cheques") or 0),
            "ativo_estoque": float(r.get("ativo_estoque") or 0),
            "ativo_estoque_combustivel": float(r.get("ativo_estoque_combustivel") or 0),
            "ativo_estoque_loja": float(r.get("ativo_estoque_loja") or 0),
            "ativo_cartoes_credito": float(r.get("ativo_cartoes_credito") or 0),
            "ativo_cartoes_debito": float(r.get("ativo_cartoes_debito") or 0),
            "tem_ativo_dados": 1 if r.get("tem_ativo_dados") else 0,
            "estoque_combustivel_medido": 1 if r.get("estoque_combustivel_medido") else 0,
            "estoque_data_leitura": r.get("estoque_data_leitura"),
            "published_at": pub,
        } for r in liq],
        order_by=["id_empresa", "id_filial", "ano_mes"],
    )

    n_aprazo = publish_solvencia_aprazo_mes(id_empresa)
    return {"itens": n_item, "bancos": n_banco, "liquidez": n_liq, "aprazo": n_aprazo}


def publish_solvencia_aprazo_mes(id_empresa: int) -> int:
    """Materializa a prazo por mês direto no ClickHouse (stg_contasreceber → mart)."""
    from app.db_clickhouse import execute_command, query_scalar

    # INSERT SELECT no próprio CH — sem round-trip de 650k títulos pela API.
    execute_command(
        f"""
        INSERT INTO torqmind_mart_rt.mart_solvencia_aprazo_mes
          (id_empresa, id_filial, ano_mes, valor, qtd, published_at)
        SELECT
          id_empresa,
          id_filial,
          toYYYYMM(dt_vcto) AS ano_mes,
          sum(greatest(saldo_aberto, 0)) AS valor,
          toInt32(count()) AS qtd,
          now64(3)
        FROM (
          SELECT
            id_empresa,
            id_filial,
            toDateOrNull(substring(JSONExtractString(payload, 'DTAVCTO'), 1, 10)) AS dt_vcto,
            toFloat64OrZero(JSONExtractString(payload, 'VALOR'))
              - toFloat64OrZero(JSONExtractString(payload, 'VLRPAGO')) AS saldo_aberto
          FROM torqmind_current.stg_contasreceber FINAL
          WHERE id_empresa = {int(id_empresa)}
            AND is_deleted = 0
            AND JSONExtractString(payload, 'DTAPGTO') = ''
        )
        WHERE dt_vcto IS NOT NULL
          AND toYYYYMM(dt_vcto) >= toYYYYMM(addMonths(today(), -24))
        GROUP BY id_empresa, id_filial, ano_mes
        """
    )
    n = query_scalar(
        f"""
        SELECT count() FROM torqmind_mart_rt.mart_solvencia_aprazo_mes FINAL
        WHERE id_empresa = {int(id_empresa)}
        """
    )
    return int(n or 0)


def publish_fraud_credito_cliente(role: str, id_empresa: int, days: int = 400) -> Dict[str, int]:
    """Publica movimentos + saldo de crédito cliente (antifraude)."""
    pub = _now()
    movs = _fetch(
        role,
        id_empresa,
        """
        SELECT
          m.id_empresa, m.id_filial, COALESCE(m.id_db, 0) AS id_db,
          COALESCE(NULLIF(m.payload->>'ID_MOVCREDITOENTIDADES', '')::bigint, 0) AS id_mov,
          COALESCE(NULLIF(m.payload->>'ID_ENTIDADE', '')::int, 0) AS id_entidade,
          COALESCE(NULLIF(m.payload->>'ID_USUARIOS', '')::int, 0) AS id_usuario,
          COALESCE(
            NULLIF(TRIM(u.payload->>'NOMEUSUARIOS'), ''),
            NULLIF(TRIM(u.payload->>'NOME'), ''),
            ''
          ) AS operador,
          LEFT(m.payload->>'DATA', 10)::date AS data_dia,
          COALESCE(m.payload->>'DATA', '') AS data_raw,
          COALESCE((m.payload->>'ENTRADAS')::numeric, 0) AS entradas,
          COALESCE((m.payload->>'SAIDAS')::numeric, 0) AS saidas,
          COALESCE(NULLIF(TRIM(m.payload->>'HISTORICO'), ''), '') AS historico,
          COALESCE(NULLIF(TRIM(m.payload->>'REFERENCIA'), ''), '') AS referencia,
          (m.payload->>'HISTORICO' ILIKE '%%adicionado manualmente%%') AS manual_suspeita
        FROM stg.movcreditoentidades m
        LEFT JOIN stg.usuarios u
          ON u.id_empresa = m.id_empresa
         AND u.id_usuario = NULLIF(m.payload->>'ID_USUARIOS', '')::int
        WHERE m.id_empresa = %s
          AND LEFT(m.payload->>'DATA', 10)::date >= (CURRENT_DATE - (%s || ' days')::interval)
          AND (
            COALESCE((m.payload->>'ENTRADAS')::numeric, 0) > 0
            OR COALESCE((m.payload->>'SAIDAS')::numeric, 0) > 0
          )
        """,
        [id_empresa, int(days)],
    )
    n_mov = insert_batch(
        "torqmind_mart_rt.mart_fraud_credito_cliente_mov",
        [{
            "id_empresa": int(r["id_empresa"]),
            "id_filial": int(r["id_filial"]),
            "id_db": int(r.get("id_db") or 0),
            "id_mov": int(r.get("id_mov") or 0),
            "id_entidade": int(r.get("id_entidade") or 0),
            "id_usuario": int(r.get("id_usuario") or 0),
            "operador": str(r.get("operador") or ""),
            "data_dia": r["data_dia"],
            "data_raw": str(r.get("data_raw") or ""),
            "entradas": float(r.get("entradas") or 0),
            "saidas": float(r.get("saidas") or 0),
            "historico": str(r.get("historico") or ""),
            "referencia": str(r.get("referencia") or ""),
            "manual_suspeita": 1 if r.get("manual_suspeita") else 0,
            "published_at": pub,
        } for r in movs if r.get("data_dia") and int(r.get("id_entidade") or 0) > 0],
        order_by=["id_empresa", "id_filial", "id_entidade", "id_mov"],
    )

    saldos = _fetch(
        role,
        id_empresa,
        """
        SELECT id_empresa, id_filial,
               (payload->>'ID_ENTIDADE')::int AS id_entidade,
               SUM(COALESCE((payload->>'SALDO')::numeric, 0)) AS saldo
        FROM stg.credito
        WHERE id_empresa = %s
          AND NULLIF(payload->>'ID_ENTIDADE', '') IS NOT NULL
        GROUP BY 1, 2, 3
        """,
        [id_empresa],
    )
    n_sal = insert_batch(
        "torqmind_mart_rt.mart_fraud_credito_cliente_saldo",
        [{
            "id_empresa": int(r["id_empresa"]),
            "id_filial": int(r["id_filial"]),
            "id_entidade": int(r["id_entidade"]),
            "saldo": float(r.get("saldo") or 0),
            "published_at": pub,
        } for r in saldos],
        order_by=["id_empresa", "id_filial", "id_entidade"],
    )
    return {"movs": n_mov, "saldos": n_sal}


def publish_all_hotpath(role: str = "platform_master", id_empresa: int = 1) -> Dict[str, Any]:
    """Publica todas as marts hot-path para uma empresa."""
    out: Dict[str, Any] = {}
    out["despesa_conta"] = publish_despesa_conta_mensal(role, id_empresa)
    out["plano_contas"] = publish_plano_contas_gerencial(role, id_empresa)
    out["cheques"] = publish_cheques_pendentes(role, id_empresa)
    out["ticket_combustivel"] = publish_ticket_combustivel(role, id_empresa)
    out["solvencia"] = publish_solvencia_itens(role, id_empresa)
    out["fraud_credito_cliente"] = publish_fraud_credito_cliente(role, id_empresa)
    # Crédito funcionário: mash PG + publish CH (mês corrente SP).
    try:
        from app.repos_mart import refresh_fraud_credito_funcionario

        out["fraud_credito_funcionario"] = refresh_fraud_credito_funcionario(role, id_empresa)
    except Exception as exc:
        logger.warning("fraud_credito_funcionario publish skipped: %s", str(exc)[:200])
        out["fraud_credito_funcionario"] = {"error": str(exc)[:200]}
    try:
        from app.services.cliente_preco_fixo import publish_and_rebuild

        out["cliente_preco_fixo"] = publish_and_rebuild(role, id_empresa, days=120)
    except Exception as exc:
        logger.warning("cliente_preco_fixo publish skipped: %s", str(exc)[:200])
        out["cliente_preco_fixo"] = {"error": str(exc)[:200]}
    try:
        from app.repos_product_management import refresh_and_publish_product_stock_idle

        out["product_stock_idle"] = refresh_and_publish_product_stock_idle(role, id_empresa)
    except Exception as exc:
        logger.warning("product_stock_idle publish skipped: %s", str(exc)[:200])
        out["product_stock_idle"] = {"error": str(exc)[:200]}
    logger.info("publish_all_hotpath empresa=%s %s", id_empresa, out)
    return out


if __name__ == "__main__":
    import json
    import sys

    emp = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    print(json.dumps(publish_all_hotpath("platform_master", emp), default=str))
