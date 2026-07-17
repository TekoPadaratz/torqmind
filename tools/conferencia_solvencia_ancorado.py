#!/usr/bin/env python3
"""Números de confiança: combustível as-of (movtanques) + estoque loja ancorado."""
from app.db import get_conn

E, F = 1, 14458
GRUPOS_LOJA = [2, 4, 7, 8, 9, 16, 39, 10, 14, 15, 18, 12, 13, 11, 17, 21, 37, 40, 41, 19, 20]


def main() -> None:
    with get_conn(role="MASTER", tenant_id=E, branch_id=F) as conn:
        conn.execute("SET statement_timeout = '300s'")

        print("=== Combustível as-of (join tipado tanque.id_tanque = mov.ID_TANQUES) ===")
        rows = conn.execute(
            """
            WITH cortes AS (
              SELECT 202606 AS ym, DATE '2026-06-01' AS corte
              UNION ALL SELECT 202607, DATE '2026-07-01'
              UNION ALL SELECT 209901, (now() AT TIME ZONE 'America/Sao_Paulo')::date
            ),
            ult AS (
              SELECT c.ym, t.id_tanque, etl.safe_int(t.payload->>'ID_PRODUTOS') AS id_produto,
                (
                  SELECT GREATEST(etl.safe_numeric(m.payload->>'LEITURA'), 0)
                  FROM stg.movtanques m
                  WHERE m.id_empresa = t.id_empresa AND m.id_filial = t.id_filial
                    AND etl.safe_int(m.payload->>'ID_TANQUES') = t.id_tanque
                    AND COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DTACONTA')) < c.corte
                  ORDER BY COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DTACONTA')) DESC NULLS LAST
                  LIMIT 1
                ) AS litros
              FROM cortes c
              CROSS JOIN stg.tanques t
              WHERE t.id_empresa = %s AND t.id_filial = %s
            )
            SELECT u.ym,
                   ROUND(SUM(COALESCE(u.litros,0))::numeric,2) AS litros,
                   ROUND(SUM(COALESCE(u.litros,0) * COALESCE(NULLIF(etl.safe_numeric(p.payload->>'CUSTOMEDIO'),0),0))::numeric,2) AS valor,
                   COUNT(*) FILTER (WHERE u.litros IS NULL) AS sem_leitura
            FROM ult u
            LEFT JOIN stg.produtos p
              ON p.id_empresa = %s AND p.id_filial = %s
             AND etl.safe_int(p.payload->>'ID_PRODUTOS') = u.id_produto
            GROUP BY u.ym
            ORDER BY 1
            """,
            [E, F, E, F],
        ).fetchall()
        for r in rows:
            print(dict(r))

        print("\n=== Estoque loja HOJE (whitelist grupos mercadoria, exclui tanque) ===")
        r = conn.execute(
            """
            SELECT ROUND(SUM(
                     GREATEST(COALESCE(e.quantidade,0),0)
                     * COALESCE(NULLIF(etl.safe_numeric(pr.payload->>'CUSTOMEDIO'),0), e.custo_medio, 0)
                   )::numeric,2) AS valor,
                   COUNT(*) FILTER (WHERE e.quantidade > 0) AS skus
            FROM stg.estoque e
            JOIN stg.produtos pr
              ON pr.id_empresa = e.id_empresa AND pr.id_filial = e.id_filial AND pr.id_produto = e.id_produto
            WHERE e.id_empresa = %s AND e.id_filial = %s
              AND (pr.payload->>'ID_GRUPOPRODUTOS')::int = ANY(%s)
              AND NOT EXISTS (
                SELECT 1 FROM stg.tanques t
                WHERE t.id_empresa = e.id_empresa AND t.id_filial = e.id_filial
                  AND (t.payload->>'ID_PRODUTOS')::int = e.id_produto
              )
            """,
            [E, F, GRUPOS_LOJA],
        ).fetchone()
        print(dict(r))

        print("\n=== Estoque loja as-of ANCORADO (hoje − mov após o corte) ===")
        for ym, corte in [(202606, "2026-06-01"), (202607, "2026-07-01")]:
            r = conn.execute(
                """
                WITH base AS (
                  SELECT e.id_produto,
                         GREATEST(COALESCE(e.quantidade,0),0) AS q_hoje,
                         COALESCE(NULLIF(etl.safe_numeric(pr.payload->>'CUSTOMEDIO'),0), e.custo_medio, 0) AS custo
                  FROM stg.estoque e
                  JOIN stg.produtos pr
                    ON pr.id_empresa = e.id_empresa AND pr.id_filial = e.id_filial AND pr.id_produto = e.id_produto
                  WHERE e.id_empresa = %s AND e.id_filial = %s
                    AND (pr.payload->>'ID_GRUPOPRODUTOS')::int = ANY(%s)
                    AND NOT EXISTS (
                      SELECT 1 FROM stg.tanques t
                      WHERE t.id_empresa = e.id_empresa AND t.id_filial = e.id_filial
                        AND (t.payload->>'ID_PRODUTOS')::int = e.id_produto
                    )
                ),
                delta AS (
                  SELECT COALESCE(etl.safe_int(i.payload->>'ID_PRODUTOS'), i.id_produto_shadow) AS id_produto,
                    SUM(CASE
                      WHEN COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow, -1) = 1
                        THEN COALESCE(etl.safe_numeric(i.payload->>'QTDE'), i.qtd_shadow, 0)
                      WHEN COALESCE(etl.safe_int(m.payload->>'SAIDAS_ENTRADAS'), m.saidas_entradas_shadow, -1) = 0
                        THEN -COALESCE(etl.safe_numeric(i.payload->>'QTDE'), i.qtd_shadow, 0)
                      ELSE 0
                    END)::numeric AS q_pos
                  FROM stg.itensmovprodutos i
                  JOIN stg.movprodutos m
                    ON m.id_empresa = i.id_empresa AND m.id_filial = i.id_filial AND m.id_db = i.id_db
                   AND m.id_movprodutos = COALESCE(etl.safe_int(i.payload->>'ID_MOVPRODUTOS'), NULL)
                   AND COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DATA')) >= %s::date
                   AND COALESCE(m.dt_evento, etl.safe_timestamp(m.payload->>'DATA'))
                         < (now() AT TIME ZONE 'America/Sao_Paulo')
                  WHERE i.id_empresa = %s AND i.id_filial = %s
                  GROUP BY 1
                )
                SELECT
                  ROUND(SUM(GREATEST(b.q_hoje - COALESCE(d.q_pos,0), 0) * b.custo)
                        FILTER (WHERE b.custo > 0)::numeric, 2) AS asof_ancorado,
                  ROUND(SUM(b.q_hoje * b.custo) FILTER (WHERE b.custo > 0)::numeric, 2) AS hoje
                FROM base b
                LEFT JOIN delta d ON d.id_produto = b.id_produto
                """,
                [E, F, GRUPOS_LOJA, corte, E, F],
            ).fetchone()
            print(ym, dict(r))


if __name__ == "__main__":
    main()
