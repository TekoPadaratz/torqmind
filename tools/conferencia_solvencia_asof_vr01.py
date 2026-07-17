#!/usr/bin/env python3
"""Conferência Solvência as-of — homolog VR01 (14458). Somente leitura."""

from __future__ import annotations

from app.db import get_conn

E, F = 1, 14458


def main() -> None:
    with get_conn(role="MASTER", tenant_id=E, branch_id=F) as conn:
        print("=== 1) CHEQUES: snapshot vs liquidez (causa do sumiço) ===")
        snap = conn.execute(
            """
            SELECT ROUND(SUM(valor)::numeric,2) AS tot, COUNT(*) AS n
            FROM mart.solvencia_item
            WHERE id_empresa=%s AND id_filial=%s AND secao='cheques'
            """,
            [E, F],
        ).fetchone()
        liq = conn.execute(
            """
            SELECT ano_mes, ativo_cheques, ativo_estoque_loja, ativo_estoque_combustivel
            FROM mart.liquidez_solvencia
            WHERE id_empresa=%s AND id_filial=%s AND ano_mes IN (202606,202607)
            ORDER BY 1
            """,
            [E, F],
        ).fetchall()
        stg_ch = conn.execute(
            """
            SELECT COUNT(*) AS n,
                   ROUND(SUM(etl.safe_numeric(payload->>'VALOR'))::numeric,2) AS tot
            FROM stg.cheques
            WHERE id_empresa=%s AND id_filial=%s
              AND payload->>'DTACOMPENSADO' IS NULL
              AND COALESCE(payload->>'SITUACAOCHEQUE','0') <> '2'
              AND etl.safe_numeric(payload->>'VALOR') > 0
            """,
            [E, F],
        ).fetchone()
        print("snapshot_cheques", dict(snap))
        print("stg_cheques_abertos", dict(stg_ch))
        print("liquidez_ativo_cheques", [dict(r) for r in liq])

        print("\n=== 2) ESTOQUE LOJA: as-of mart vs ESTOQUE Xpert (snapshot) ===")
        est_snap = conn.execute(
            """
            SELECT ROUND(SUM(valor)::numeric,2) AS tot
            FROM mart.solvencia_item
            WHERE id_empresa=%s AND id_filial=%s AND secao='estoque'
            """,
            [E, F],
        ).fetchone()
        print("snapshot_estoque_loja_atual", dict(est_snap))

        # Reconstrução MOV "agora" vs stg.estoque (mesmo método da 108)
        cmp = conn.execute(
            """
            WITH corte AS (
              SELECT (now() AT TIME ZONE 'America/Sao_Paulo')::date AS d
            ),
            mov AS (
              SELECT
                COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow) AS id_produto,
                SUM(
                  CASE
                    WHEN COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow, -1) = 1
                      THEN COALESCE(etl.safe_numeric(i.payload->>'QTDE'), i.qtd_shadow, 0)
                    WHEN COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow, -1) = 0
                      THEN -COALESCE(etl.safe_numeric(i.payload->>'QTDE'), i.qtd_shadow, 0)
                    ELSE 0
                  END
                )::numeric(18,6) AS qtde_asof
              FROM stg.itensmovprodutos i
              JOIN stg.movprodutos m
                ON m.id_empresa = i.id_empresa AND m.id_filial = i.id_filial AND m.id_db = i.id_db
               AND m.id_movprodutos = COALESCE(etl.safe_int(i.payload->>'ID_MOVPRODUTOS'), NULL)
               AND COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DATA')) < (SELECT d FROM corte)
              WHERE i.id_empresa = %s AND i.id_filial = %s
                AND NOT EXISTS (
                  SELECT 1 FROM stg.tanques t
                  WHERE t.id_empresa = i.id_empresa AND t.id_filial = i.id_filial
                    AND etl.safe_int(t.payload->>'ID_PRODUTOS')
                        = COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow)
                )
              GROUP BY 1
            ),
            custo AS (
              SELECT DISTINCT ON (pid)
                pid AS id_produto,
                custo_unit
              FROM (
                SELECT
                  COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow) AS pid,
                  etl.safe_numeric(i.payload->>'VLRCUSTOCOMICMS') AS custo_unit,
                  COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DATA')) AS dt
                FROM stg.itensmovprodutos i
                JOIN stg.movprodutos m
                  ON m.id_empresa = i.id_empresa AND m.id_filial = i.id_filial AND m.id_db = i.id_db
                 AND m.id_movprodutos = COALESCE(etl.safe_int(i.payload->>'ID_MOVPRODUTOS'), NULL)
                 AND COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DATA'))
                       < (SELECT d FROM corte)
                WHERE i.id_empresa = %s AND i.id_filial = %s
                  AND (
                    COALESCE(i.payload->>'CFOP','') LIKE '1.%%'
                    OR COALESCE(i.payload->>'CFOP','') LIKE '2.%%'
                    OR COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow) = 1
                  )
                  AND etl.safe_numeric(i.payload->>'VLRCUSTOCOMICMS') > 0
              ) s
              ORDER BY pid, dt DESC
            ),
            xpert AS (
              SELECT etl.safe_int(e.payload->>'ID_PRODUTOS') AS id_produto,
                     SUM(etl.safe_numeric(e.payload->>'QTDEATUAL'))::numeric AS qtde
              FROM stg.estoque e
              WHERE e.id_empresa = %s AND e.id_filial = %s
              GROUP BY 1
            ),
            cust_prod AS (
              SELECT DISTINCT ON (etl.safe_int(p.payload->>'ID_PRODUTOS'))
                etl.safe_int(p.payload->>'ID_PRODUTOS') AS id_produto,
                COALESCE(
                  NULLIF(etl.safe_numeric(p.payload->>'CUSTOGERENCIAL'),0),
                  NULLIF(etl.safe_numeric(p.payload->>'CUSTOMEDIO'),0),
                  NULLIF(etl.safe_numeric(p.payload->>'ULTVLRENTRADA'),0),
                  0
                ) AS custo_prod
              FROM stg.produtos p
              WHERE p.id_empresa = %s AND p.id_filial = %s
              ORDER BY etl.safe_int(p.payload->>'ID_PRODUTOS')
            )
            SELECT
              ROUND(SUM(GREATEST(m.qtde_asof,0) * COALESCE(c.custo_unit,0))
                    FILTER (WHERE m.qtde_asof > 0 AND COALESCE(c.custo_unit,0) > 0)::numeric, 2)
                AS valoriz_mov_custo_nf,
              ROUND(SUM(GREATEST(x.qtde,0) * COALESCE(NULLIF(c.custo_unit,0), cp.custo_prod, 0))
                    FILTER (WHERE x.qtde <> 0)::numeric, 2)
                AS valoriz_xpert_custo_nf_ou_prod,
              ROUND(SUM(GREATEST(x.qtde,0) * COALESCE(cp.custo_prod,0))
                    FILTER (WHERE x.qtde <> 0 AND COALESCE(cp.custo_prod,0) > 0)::numeric, 2)
                AS valoriz_xpert_custo_produto,
              COUNT(*) FILTER (WHERE m.qtde_asof > 0 AND COALESCE(x.qtde,0) <= 0 AND COALESCE(c.custo_unit,0) > 0)
                AS skus_fantasma_mov_sem_estoque,
              ROUND(SUM(GREATEST(m.qtde_asof,0) * c.custo_unit)
                    FILTER (WHERE m.qtde_asof > 0 AND COALESCE(x.qtde,0) <= 0 AND c.custo_unit > 0)::numeric, 2)
                AS valor_fantasma,
              COUNT(*) FILTER (WHERE COALESCE(x.qtde,0) > 0 AND COALESCE(m.qtde_asof,0) <= 0)
                AS skus_xpert_nao_no_mov
            FROM mov m
            FULL OUTER JOIN xpert x ON x.id_produto = m.id_produto
            LEFT JOIN custo c ON c.id_produto = COALESCE(m.id_produto, x.id_produto)
            LEFT JOIN cust_prod cp ON cp.id_produto = COALESCE(m.id_produto, x.id_produto)
            """,
            [E, F, E, F, E, F, E, F],
        ).fetchone()
        print("recon_hoje", dict(cmp))

        top = conn.execute(
            """
            WITH corte AS (
              SELECT (now() AT TIME ZONE 'America/Sao_Paulo')::date AS d
            ),
            mov AS (
              SELECT
                COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow) AS id_produto,
                SUM(
                  CASE
                    WHEN COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow, -1) = 1
                      THEN COALESCE(etl.safe_numeric(i.payload->>'QTDE'), i.qtd_shadow, 0)
                    WHEN COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow, -1) = 0
                      THEN -COALESCE(etl.safe_numeric(i.payload->>'QTDE'), i.qtd_shadow, 0)
                    ELSE 0
                  END
                )::numeric(18,6) AS qtde_asof
              FROM stg.itensmovprodutos i
              JOIN stg.movprodutos m
                ON m.id_empresa = i.id_empresa AND m.id_filial = i.id_filial AND m.id_db = i.id_db
               AND m.id_movprodutos = COALESCE(etl.safe_int(i.payload->>'ID_MOVPRODUTOS'), NULL)
               AND COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DATA')) < (SELECT d FROM corte)
              WHERE i.id_empresa = %s AND i.id_filial = %s
                AND NOT EXISTS (
                  SELECT 1 FROM stg.tanques t
                  WHERE t.id_empresa = i.id_empresa AND t.id_filial = i.id_filial
                    AND etl.safe_int(t.payload->>'ID_PRODUTOS')
                        = COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow)
                )
              GROUP BY 1
            ),
            custo AS (
              SELECT DISTINCT ON (pid) pid AS id_produto, custo_unit, nome
              FROM (
                SELECT
                  COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow) AS pid,
                  etl.safe_numeric(i.payload->>'VLRCUSTOCOMICMS') AS custo_unit,
                  COALESCE(NULLIF(p.payload->>'NOMEPRODUTO',''), p.payload->>'PRODUTO', '?') AS nome,
                  COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DATA')) AS dt
                FROM stg.itensmovprodutos i
                JOIN stg.movprodutos m
                  ON m.id_empresa = i.id_empresa AND m.id_filial = i.id_filial AND m.id_db = i.id_db
                 AND m.id_movprodutos = COALESCE(etl.safe_int(i.payload->>'ID_MOVPRODUTOS'), NULL)
                LEFT JOIN stg.produtos p
                  ON p.id_empresa = i.id_empresa AND p.id_filial = i.id_filial
                 AND etl.safe_int(p.payload->>'ID_PRODUTOS')
                     = COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow)
                WHERE i.id_empresa = %s AND i.id_filial = %s
                  AND etl.safe_numeric(i.payload->>'VLRCUSTOCOMICMS') > 0
              ) s
              ORDER BY pid, dt DESC NULLS LAST
            ),
            xpert AS (
              SELECT etl.safe_int(e.payload->>'ID_PRODUTOS') AS id_produto,
                     SUM(etl.safe_numeric(e.payload->>'QTDEATUAL'))::numeric AS qtde
              FROM stg.estoque e
              WHERE e.id_empresa = %s AND e.id_filial = %s
              GROUP BY 1
            )
            SELECT m.id_produto, LEFT(c.nome, 42) AS nome,
                   ROUND(m.qtde_asof::numeric, 2) AS q_mov,
                   ROUND(COALESCE(x.qtde, 0)::numeric, 2) AS q_xpert,
                   ROUND(c.custo_unit::numeric, 2) AS custo,
                   ROUND((GREATEST(m.qtde_asof, 0) * c.custo_unit)::numeric, 2) AS valor_fantasma
            FROM mov m
            LEFT JOIN xpert x ON x.id_produto = m.id_produto
            JOIN custo c ON c.id_produto = m.id_produto
            WHERE m.qtde_asof > 0 AND COALESCE(x.qtde, 0) <= 0 AND c.custo_unit > 0
            ORDER BY GREATEST(m.qtde_asof, 0) * c.custo_unit DESC
            LIMIT 12
            """,
            [E, F, E, F, E, F],
        ).fetchall()
        print("top_skus_fantasma_mov")
        for r in top:
            print(dict(r))

        print("\n=== 3) COMBUSTÍVEL: origem e histórico ===")
        comb = conn.execute(
            """
            SELECT item_label, ROUND(valor::numeric,2) valor, ROUND(qtd::numeric,2) litros
            FROM mart.solvencia_item
            WHERE id_empresa=%s AND id_filial=%s AND secao='combustivel'
            ORDER BY valor DESC
            """,
            [E, F],
        ).fetchall()
        print("snapshot_combustivel_por_tipo", [dict(r) for r in comb])

        # Há mov de combustível em movprodutos?
        fuel_mov = conn.execute(
            """
            SELECT COUNT(*) AS n_itens,
                   ROUND(SUM(COALESCE(etl.safe_numeric(i.payload->>'QTDE'),0))::numeric,2) AS qtde
            FROM stg.itensmovprodutos i
            JOIN stg.tanques t
              ON t.id_empresa = i.id_empresa AND t.id_filial = i.id_filial
             AND etl.safe_int(t.payload->>'ID_PRODUTOS')
                 = COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow)
            WHERE i.id_empresa=%s AND i.id_filial=%s
            """,
            [E, F],
        ).fetchone()
        print("itens_mov_de_produtos_de_tanque", dict(fuel_mov))

        mt = conn.execute(
            """
            SELECT
              COUNT(*) AS n_leituras,
              MIN(etl.safe_timestamp(payload->>'DTACONTA')) AS min_dt,
              MAX(etl.safe_timestamp(payload->>'DTACONTA')) AS max_dt
            FROM stg.movtanques
            WHERE id_empresa=%s AND id_filial=%s
            """,
            [E, F],
        ).fetchone()
        print("movtanques_cobertura", dict(mt))

        # Leitura as-of 01/06 e 01/07 vs atual (última por tanque)
        asof_comb = conn.execute(
            """
            WITH cortes AS (
              SELECT 202606 AS ano_mes, DATE '2026-06-01' AS corte
              UNION ALL SELECT 202607, DATE '2026-07-01'
              UNION ALL SELECT 202699, (now() AT TIME ZONE 'America/Sao_Paulo')::date
            ),
            tanques AS (
              SELECT t.payload->>'ID_TANQUES' AS id_tanque,
                     etl.safe_int(t.payload->>'ID_PRODUTOS') AS id_produto
              FROM stg.tanques t
              WHERE t.id_empresa=%s AND t.id_filial=%s
            ),
            leituras AS (
              SELECT c.ano_mes, c.corte, tn.id_produto,
                     (
                       SELECT GREATEST(etl.safe_numeric(m.payload->>'LEITURA'),0)
                       FROM stg.movtanques m
                       WHERE m.id_empresa=%s AND m.id_filial=%s
                         AND m.payload->>'ID_TANQUES' = tn.id_tanque
                         AND etl.safe_timestamp(m.payload->>'DTACONTA') < c.corte
                       ORDER BY etl.safe_timestamp(m.payload->>'DTACONTA') DESC NULLS LAST
                       LIMIT 1
                     ) AS litros
              FROM cortes c
              CROSS JOIN tanques tn
            ),
            custo AS (
              SELECT DISTINCT ON (etl.safe_int(p.payload->>'ID_PRODUTOS'))
                etl.safe_int(p.payload->>'ID_PRODUTOS') AS id_produto,
                COALESCE(
                  NULLIF(etl.safe_numeric(p.payload->>'CUSTOGERENCIAL'),0),
                  NULLIF(etl.safe_numeric(p.payload->>'CUSTOMEDIO'),0),
                  NULLIF(etl.safe_numeric(p.payload->>'ULTVLRENTRADA'),0),
                  0
                ) AS custo,
                COALESCE(NULLIF(p.payload->>'NOMEPRODUTO',''), '?') AS nome
              FROM stg.produtos p
              WHERE p.id_empresa=%s AND p.id_filial=%s
              ORDER BY etl.safe_int(p.payload->>'ID_PRODUTOS')
            )
            SELECT l.ano_mes,
                   ROUND(SUM(COALESCE(l.litros,0) * COALESCE(c.custo,0))::numeric,2) AS valor,
                   ROUND(SUM(COALESCE(l.litros,0))::numeric,2) AS litros
            FROM leituras l
            LEFT JOIN custo c ON c.id_produto = l.id_produto
            GROUP BY l.ano_mes
            ORDER BY 1
            """,
            [E, F, E, F, E, F],
        ).fetchall()
        print("combustivel_por_movtanques_asof", [dict(r) for r in asof_comb])

        print("\n=== 4) ANCORAGEM: ESTOQUE hoje − movimentos pós-corte (método confiável) ===")
        for am, corte in [(202606, "2026-06-01"), (202607, "2026-07-01")]:
            r = conn.execute(
                """
                WITH corte AS (SELECT %s::date AS d),
                xpert AS (
                  SELECT etl.safe_int(e.payload->>'ID_PRODUTOS') AS id_produto,
                         SUM(etl.safe_numeric(e.payload->>'QTDEATUAL'))::numeric AS qtde_hoje
                  FROM stg.estoque e
                  WHERE e.id_empresa=%s AND e.id_filial=%s
                  GROUP BY 1
                ),
                delta AS (
                  SELECT
                    COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow) AS id_produto,
                    SUM(
                      CASE
                        WHEN COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow, -1) = 1
                          THEN COALESCE(etl.safe_numeric(i.payload->>'QTDE'), i.qtd_shadow, 0)
                        WHEN COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow, -1) = 0
                          THEN -COALESCE(etl.safe_numeric(i.payload->>'QTDE'), i.qtd_shadow, 0)
                        ELSE 0
                      END
                    )::numeric(18,6) AS qtde_pos_corte
                  FROM stg.itensmovprodutos i
                  JOIN stg.movprodutos m
                    ON m.id_empresa=i.id_empresa AND m.id_filial=i.id_filial AND m.id_db=i.id_db
                   AND m.id_movprodutos=COALESCE(etl.safe_int(i.payload->>'ID_MOVPRODUTOS'), NULL)
                   AND COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DATA')) >= (SELECT d FROM corte)
                   AND COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DATA'))
                         < (now() AT TIME ZONE 'America/Sao_Paulo')
                  WHERE i.id_empresa=%s AND i.id_filial=%s
                    AND NOT EXISTS (
                      SELECT 1 FROM stg.tanques t
                      WHERE t.id_empresa=i.id_empresa AND t.id_filial=i.id_filial
                        AND etl.safe_int(t.payload->>'ID_PRODUTOS')
                            = COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow)
                    )
                  GROUP BY 1
                ),
                cust_prod AS (
                  SELECT DISTINCT ON (etl.safe_int(p.payload->>'ID_PRODUTOS'))
                    etl.safe_int(p.payload->>'ID_PRODUTOS') AS id_produto,
                    COALESCE(
                      NULLIF(etl.safe_numeric(p.payload->>'CUSTOGERENCIAL'),0),
                      NULLIF(etl.safe_numeric(p.payload->>'CUSTOMEDIO'),0),
                      NULLIF(etl.safe_numeric(p.payload->>'ULTVLRENTRADA'),0),
                      0
                    ) AS custo
                  FROM stg.produtos p
                  WHERE p.id_empresa=%s AND p.id_filial=%s
                  ORDER BY etl.safe_int(p.payload->>'ID_PRODUTOS')
                )
                SELECT
                  ROUND(SUM(GREATEST(x.qtde_hoje - COALESCE(d.qtde_pos_corte,0), 0) * c.custo)
                        FILTER (WHERE c.custo > 0)::numeric, 2) AS estoque_asof_ancorado,
                  ROUND(SUM(GREATEST(x.qtde_hoje,0) * c.custo)
                        FILTER (WHERE c.custo > 0)::numeric, 2) AS estoque_hoje_valorizado
                FROM xpert x
                LEFT JOIN delta d ON d.id_produto = x.id_produto
                JOIN cust_prod c ON c.id_produto = x.id_produto
                """,
                [corte, E, F, E, F, E, F],
            ).fetchone()
            print(f"ancorado_{am}", dict(r))


if __name__ == "__main__":
    main()
