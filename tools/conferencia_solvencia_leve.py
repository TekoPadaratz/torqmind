#!/usr/bin/env python3
"""Conferência leve Solvência — cheques, estoque, combustível."""
from app.db import get_conn

E, F = 1, 14458


def main() -> None:
    with get_conn(role="MASTER", tenant_id=E, branch_id=F) as conn:
        conn.execute("SET statement_timeout = '300s'")

        print("=== CHEQUES STG (mesma regra 106) vs mart ===")
        stg = conn.execute(
            """
            SELECT
              'Banco ' || COALESCE(NULLIF(c.payload->>'CODIGOBANCOSPADRAO',''),'?') AS item_label,
              SUM(etl.safe_numeric(c.payload->>'VALOR'))::numeric(18,2) AS valor,
              COUNT(*)::int AS qtd
            FROM stg.cheques c
            WHERE c.id_empresa = %s AND c.id_filial = %s
              AND c.payload->>'DTACOMPENSADO' IS NULL
              AND COALESCE(c.payload->>'SITUACAOCHEQUE','0') <> '2'
              AND etl.safe_numeric(c.payload->>'VALOR') > 0
            GROUP BY c.payload->>'CODIGOBANCOSPADRAO'
            ORDER BY 2 DESC
            """,
            [E, F],
        ).fetchall()
        print("stg", [dict(r) for r in stg], "SUM", round(sum(float(r["valor"]) for r in stg), 2))
        mart = conn.execute(
            """
            SELECT item_label, valor, qtd FROM mart.solvencia_item
            WHERE id_empresa=%s AND id_filial=%s AND secao='cheques'
            ORDER BY valor DESC
            """,
            [E, F],
        ).fetchall()
        print("mart", [dict(r) for r in mart], "SUM", round(sum(float(r["valor"]) for r in mart), 2))

        print("\n=== ESTOQUE atual (stg.estoque x custo produto, exclui tanque) ===")
        est = conn.execute(
            """
            WITH est AS (
              SELECT etl.safe_int(e.payload->>'ID_PRODUTOS') AS id_produto,
                     SUM(etl.safe_numeric(e.payload->>'QTDEATUAL'))::numeric AS qtde
              FROM stg.estoque e
              WHERE e.id_empresa=%s AND e.id_filial=%s
              GROUP BY 1
            ),
            tanque_prod AS (
              SELECT DISTINCT etl.safe_int(t.payload->>'ID_PRODUTOS') AS id_produto
              FROM stg.tanques t WHERE t.id_empresa=%s AND t.id_filial=%s
            ),
            custo AS (
              SELECT etl.safe_int(p.payload->>'ID_PRODUTOS') AS id_produto,
                     COALESCE(
                       NULLIF(etl.safe_numeric(p.payload->>'CUSTOGERENCIAL'),0),
                       NULLIF(etl.safe_numeric(p.payload->>'CUSTOMEDIO'),0),
                       NULLIF(etl.safe_numeric(p.payload->>'ULTVLRENTRADA'),0),
                       0) AS custo,
                     COALESCE(NULLIF(p.payload->>'NOMEPRODUTO',''), '?') AS nome
              FROM stg.produtos p
              WHERE p.id_empresa=%s AND p.id_filial=%s
            )
            SELECT
              ROUND(SUM(GREATEST(e.qtde,0)*c.custo) FILTER (WHERE c.custo>0 AND tp.id_produto IS NULL)::numeric,2) AS loja_sem_tanque,
              ROUND(SUM(GREATEST(e.qtde,0)*c.custo) FILTER (WHERE c.custo>0 AND tp.id_produto IS NOT NULL)::numeric,2) AS combustivel_via_estoque,
              COUNT(*) FILTER (WHERE e.qtde<>0 AND c.custo>0 AND tp.id_produto IS NULL) AS skus_loja
            FROM est e
            JOIN custo c ON c.id_produto = e.id_produto
            LEFT JOIN tanque_prod tp ON tp.id_produto = e.id_produto
            """,
            [E, F, E, F, E, F],
        ).fetchone()
        print("estoque_xpert_hoje", dict(est))

        print("\n=== Sample: top produtos por valor no ESTOQUE loja ===")
        top = conn.execute(
            """
            WITH est AS (
              SELECT etl.safe_int(e.payload->>'ID_PRODUTOS') AS id_produto,
                     SUM(etl.safe_numeric(e.payload->>'QTDEATUAL'))::numeric AS qtde
              FROM stg.estoque e
              WHERE e.id_empresa=%s AND e.id_filial=%s
              GROUP BY 1
            ),
            tanque_prod AS (
              SELECT DISTINCT etl.safe_int(t.payload->>'ID_PRODUTOS') AS id_produto
              FROM stg.tanques t WHERE t.id_empresa=%s AND t.id_filial=%s
            ),
            custo AS (
              SELECT etl.safe_int(p.payload->>'ID_PRODUTOS') AS id_produto,
                     COALESCE(
                       NULLIF(etl.safe_numeric(p.payload->>'CUSTOGERENCIAL'),0),
                       NULLIF(etl.safe_numeric(p.payload->>'CUSTOMEDIO'),0),
                       NULLIF(etl.safe_numeric(p.payload->>'ULTVLRENTRADA'),0),
                       0) AS custo,
                     LEFT(COALESCE(NULLIF(p.payload->>'NOMEPRODUTO',''), '?'), 40) AS nome
              FROM stg.produtos p
              WHERE p.id_empresa=%s AND p.id_filial=%s
            )
            SELECT c.nome, ROUND(e.qtde::numeric,2) qtde, ROUND(c.custo::numeric,2) custo,
                   ROUND((e.qtde*c.custo)::numeric,2) valor
            FROM est e
            JOIN custo c ON c.id_produto=e.id_produto
            LEFT JOIN tanque_prod tp ON tp.id_produto=e.id_produto
            WHERE tp.id_produto IS NULL AND e.qtde>0 AND c.custo>0
            ORDER BY e.qtde*c.custo DESC
            LIMIT 10
            """,
            [E, F, E, F, E, F],
        ).fetchall()
        for r in top:
            print(dict(r))

        print("\n=== COMBUSTÍVEL via última leitura tanque as-of ===")
        comb = conn.execute(
            """
            WITH cortes AS (
              SELECT 202606 AS ano_mes, DATE '2026-06-01' AS corte
              UNION ALL SELECT 202607, DATE '2026-07-01'
              UNION ALL SELECT 209901, (now() AT TIME ZONE 'America/Sao_Paulo')::date
            ),
            tanques AS (
              SELECT t.payload->>'ID_TANQUES' AS id_tanque,
                     etl.safe_int(t.payload->>'ID_PRODUTOS') AS id_produto
              FROM stg.tanques t WHERE t.id_empresa=%s AND t.id_filial=%s
            ),
            leituras AS (
              SELECT c.ano_mes, tn.id_produto, tn.id_tanque,
                (
                  SELECT GREATEST(etl.safe_numeric(m.payload->>'LEITURA'),0)
                  FROM stg.movtanques m
                  WHERE m.id_empresa=%s AND m.id_filial=%s
                    AND m.payload->>'ID_TANQUES' = tn.id_tanque
                    AND etl.safe_timestamp(m.payload->>'DTACONTA') < c.corte
                  ORDER BY etl.safe_timestamp(m.payload->>'DTACONTA') DESC NULLS LAST
                  LIMIT 1
                ) AS litros
              FROM cortes c CROSS JOIN tanques tn
            ),
            custo AS (
              SELECT etl.safe_int(p.payload->>'ID_PRODUTOS') AS id_produto,
                     COALESCE(
                       NULLIF(etl.safe_numeric(p.payload->>'CUSTOGERENCIAL'),0),
                       NULLIF(etl.safe_numeric(p.payload->>'CUSTOMEDIO'),0),
                       NULLIF(etl.safe_numeric(p.payload->>'ULTVLRENTRADA'),0),
                       0) AS custo
              FROM stg.produtos p WHERE p.id_empresa=%s AND p.id_filial=%s
            )
            SELECT l.ano_mes,
                   ROUND(SUM(COALESCE(l.litros,0))::numeric,2) AS litros,
                   ROUND(SUM(COALESCE(l.litros,0)*COALESCE(c.custo,0))::numeric,2) AS valor,
                   COUNT(*) FILTER (WHERE l.litros IS NULL) AS tanques_sem_leitura
            FROM leituras l
            LEFT JOIN custo c ON c.id_produto = l.id_produto
            GROUP BY l.ano_mes
            ORDER BY 1
            """,
            [E, F, E, F, E, F],
        ).fetchall()
        for r in comb:
            print(dict(r))

        print("\n=== Contagens STG mov (cobertura bootstrap) ===")
        cov = conn.execute(
            """
            SELECT
              MIN(COALESCE(dt_evento, etl.safe_timestamp(payload->>'DATA'))) AS min_dt,
              MAX(COALESCE(dt_evento, etl.safe_timestamp(payload->>'DATA'))) AS max_dt,
              COUNT(*) AS n_mov
            FROM stg.movprodutos
            WHERE id_empresa=%s AND id_filial=%s
            """,
            [E, F],
        ).fetchone()
        print("movprodutos", dict(cov))
        liq = conn.execute(
            """
            SELECT ano_mes, ativo_estoque_loja, ativo_estoque_combustivel, ativo_cheques
            FROM mart.liquidez_solvencia
            WHERE id_empresa=%s AND id_filial=%s AND ano_mes IN (202606,202607)
            ORDER BY 1
            """,
            [E, F],
        ).fetchall()
        print("liquidez", [dict(r) for r in liq])


if __name__ == "__main__":
    main()
