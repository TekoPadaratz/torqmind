from app.db import get_conn


def _tenant_ids_with_source(conn, source_table):
    rows = conn.execute(f"SELECT DISTINCT id_empresa FROM {source_table} ORDER BY id_empresa").fetchall()
    return [int(row["id_empresa"]) for row in rows]


def _run_sales_etl(function_name, source_table):
    with get_conn() as conn:
        total = 0
        for tenant_id in _tenant_ids_with_source(conn, source_table):
            row = conn.execute(f"SELECT etl.{function_name}(%s) AS rows", (tenant_id,)).fetchone()
            total += int((row or {}).get("rows") or 0)
        conn.commit()
        return total


def load_movprodutos():
    return _run_sales_etl("load_fact_venda", "stg.comprovantes")


def load_itens():
    return _run_sales_etl("load_fact_venda_item", "stg.itenscomprovantes")


def load_comprovantes():
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO dw.fact_comprovante
            SELECT
                s.id_empresa,
                f.id_filial,
                s.id_db,
                s.id_comprovante,
                (s.payload->>'DATA')::date,
                (s.payload->>'VLRTOTAL')::numeric,
                s.datarepl
            FROM stg.comprovantes s
            JOIN auth.filiais f
              ON f.xpert_id_filial = s.xpert_id_filial
        """)
        conn.commit()
