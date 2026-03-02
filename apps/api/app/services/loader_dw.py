from app.db.session import get_conn
import json

def load_movprodutos():
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO dw.fact_venda
            SELECT
                s.id_empresa,
                f.id_filial,
                s.id_db,
                s.id_movprodutos,
                (s.payload->>'ID_COMPROVANTE')::bigint,
                (s.payload->>'DATA')::date,
                (s.payload->>'TOTALVENDA')::numeric,
                s.datarepl
            FROM stg.movprodutos s
            JOIN auth.filiais f
              ON f.xpert_id_filial = s.xpert_id_filial
        """)
        conn.commit()


def load_itens():
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO dw.fact_venda_item
            SELECT
                s.id_empresa,
                f.id_filial,
                s.id_db,
                s.id_movprodutos,
                s.id_itensmovprodutos,
                (s.payload->>'ID_PRODUTOS')::bigint,
                (s.payload->>'ID_FUNCIONARIOS')::bigint,
                (s.payload->>'QTDE')::numeric,
                (s.payload->>'TOTAL')::numeric,
                (s.payload->>'MARGEMBR')::numeric,
                s.datarepl
            FROM stg.itensmovprodutos s
            JOIN auth.filiais f
              ON f.xpert_id_filial = s.xpert_id_filial
        """)
        conn.commit()


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