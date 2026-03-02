"""Generate demo data (STG) and run ETL.

Usage:
  docker compose exec api python -m app.cli.demo_load

PT-BR:
- Gera dados sintéticos (últimos 14 dias) para você ver os dashboards funcionando em minutos.
- NÃO é para produção.

EN:
- Generates synthetic data (last 14 days) so you can see dashboards instantly.
- NOT for production.

Fix notes:
- psycopg3: executemany is a CURSOR method, not a Connection method.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta

from app.db import get_conn


def _j(obj) -> str:
    return json.dumps(obj, ensure_ascii=False)


def main() -> None:
    random.seed(42)

    id_empresa = 1
    id_filial = 1
    id_db = 1

    # --- Dimensions
    filiais = [
        (
            id_empresa,
            id_filial,
            _j({"ID_FILIAL": id_filial, "NOMEFILIAL": "Filial Demo", "CNPJ": "00.000.000/0001-00"}),
        )
    ]

    grupos = []
    for g in range(1, 6):
        grupos.append(
            (
                id_empresa,
                id_filial,
                g,
                _j({"ID_GRUPOPRODUTOS": g, "ID_FILIAL": id_filial, "NOMEGRUPOPRODUTOS": f"Grupo {g}"}),
            )
        )

    locais = []
    for l in range(1, 4):
        locais.append(
            (
                id_empresa,
                id_filial,
                l,
                _j({"ID_LOCALVENDAS": l, "ID_FILIAL": id_filial, "NOMELOCALVENDAS": f"Local {l}"}),
            )
        )

    produtos = []
    for p in range(1, 21):
        g = random.randint(1, 5)
        l = random.randint(1, 3)
        custo = round(random.uniform(3.0, 20.0), 4)
        produtos.append(
            (
                id_empresa,
                id_filial,
                p,
                _j(
                    {
                        "ID_PRODUTOS": p,
                        "ID_FILIAL": id_filial,
                        "NOMEPRODUTO": f"Produto {p}",
                        "ID_GRUPOPRODUTOS": g,
                        "ID_LOCALVENDAS": l,
                        "customedio": custo,
                        "UNIDADE": "UN",
                    }
                ),
            )
        )

    funcionarios = []
    for f in range(1, 8):
        funcionarios.append(
            (
                id_empresa,
                id_filial,
                f,
                _j({"ID_FUNCIONARIOS": f, "ID_FILIAL": id_filial, "NOMEFUNCIONARIO": f"Vendedor {f}"}),
            )
        )

    clientes = []
    for c in range(1, 41):
        clientes.append(
            (
                id_empresa,
                id_filial,
                c,
                _j({"ID_ENTIDADE": c, "ID_FILIAL": id_filial, "NOMEENTIDADE": f"Cliente {c}", "CNPJCPF": ""}),
            )
        )

    # --- Facts (movimentos + itens + comprovantes)
    item_rows = []
    mov_rows = []
    comp_rows = []

    start = datetime.now() - timedelta(days=13)
    id_mov = 1000
    id_item = 1
    id_comp = 5000

    for d in range(14):
        day = start + timedelta(days=d)
        for _ in range(80):
            ts = day.replace(hour=random.randint(6, 21), minute=random.randint(0, 59), second=0, microsecond=0)

            id_mov += 1
            id_comp += 1

            id_turno = random.randint(1, 3)
            id_usuario = random.randint(1, 5)
            id_cliente = random.randint(1, 40)
            id_funcionario = random.randint(1, 7)

            n_itens = random.randint(1, 4)
            total_venda = 0.0

            for _i in range(n_itens):
                id_item += 1
                id_prod = random.randint(1, 20)
                qtd = random.randint(1, 3)
                preco = round(random.uniform(5.0, 40.0), 2)
                total = round(qtd * preco, 2)
                total_venda += total

                item_payload = {
                    "ID_ITENSMOVPRODUTOS": id_item,
                    "ID_MOVPRODUTOS": id_mov,
                    "ID_FILIAL": id_filial,
                    "ID_DB": id_db,
                    "ID_PRODUTOS": id_prod,
                    "QTDE": qtd,
                    "VLRUNITARIO": preco,
                    "TOTAL": total,
                    # regra do seu mapping: venda boa = CFOP > 4999
                    "CFOP": 5102,
                    "ID_FUNCIONARIOS": id_funcionario,
                }

                item_rows.append((id_empresa, id_filial, id_db, id_mov, id_item, _j(item_payload)))

            cancelado = random.random() < 0.03

            mov_payload = {
                "ID_MOVPRODUTOS": id_mov,
                "ID_FILIAL": id_filial,
                "ID_DB": id_db,
                "DATA": ts.isoformat(sep="T", timespec="seconds"),
                "ID_USUARIOS": id_usuario,
                "ID_ENTIDADE": id_cliente,
                "ID_COMPROVANTE": id_comp,
                "ID_TURNOS": id_turno,
                "SAIDAS_ENTRADAS": 1,
                "TOTALVENDA": round(total_venda, 2),
            }
            mov_rows.append((id_empresa, id_filial, id_db, id_mov, _j(mov_payload)))

            comp_payload = {
                "ID_COMPROVANTE": id_comp,
                "ID_FILIAL": id_filial,
                "ID_DB": id_db,
                "DATA": ts.isoformat(sep="T", timespec="seconds"),
                "ID_USUARIOS": id_usuario,
                "ID_ENTIDADE": id_cliente,
                "ID_TURNOS": id_turno,
                "VLRTOTAL": round(total_venda, 2),
                "CANCELADO": cancelado,
                "SITUACAO": 1,
            }
            comp_rows.append((id_empresa, id_filial, id_db, id_comp, _j(comp_payload)))

    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO stg.filiais (id_empresa,id_filial,payload)
                    VALUES (%s,%s,%s::jsonb)
                    ON CONFLICT (id_empresa,id_filial)
                    DO UPDATE SET payload=EXCLUDED.payload, ingested_at=now()
                    """,
                    filiais,
                )

                cur.executemany(
                    """
                    INSERT INTO stg.grupoprodutos (id_empresa,id_filial,id_grupoprodutos,payload)
                    VALUES (%s,%s,%s,%s::jsonb)
                    ON CONFLICT (id_empresa,id_filial,id_grupoprodutos)
                    DO UPDATE SET payload=EXCLUDED.payload, ingested_at=now()
                    """,
                    grupos,
                )

                cur.executemany(
                    """
                    INSERT INTO stg.localvendas (id_empresa,id_filial,id_localvendas,payload)
                    VALUES (%s,%s,%s,%s::jsonb)
                    ON CONFLICT (id_empresa,id_filial,id_localvendas)
                    DO UPDATE SET payload=EXCLUDED.payload, ingested_at=now()
                    """,
                    locais,
                )

                cur.executemany(
                    """
                    INSERT INTO stg.produtos (id_empresa,id_filial,id_produto,payload)
                    VALUES (%s,%s,%s,%s::jsonb)
                    ON CONFLICT (id_empresa,id_filial,id_produto)
                    DO UPDATE SET payload=EXCLUDED.payload, ingested_at=now()
                    """,
                    produtos,
                )

                cur.executemany(
                    """
                    INSERT INTO stg.funcionarios (id_empresa,id_filial,id_funcionario,payload)
                    VALUES (%s,%s,%s,%s::jsonb)
                    ON CONFLICT (id_empresa,id_filial,id_funcionario)
                    DO UPDATE SET payload=EXCLUDED.payload, ingested_at=now()
                    """,
                    funcionarios,
                )

                cur.executemany(
                    """
                    INSERT INTO stg.entidades (id_empresa,id_filial,id_entidade,payload)
                    VALUES (%s,%s,%s,%s::jsonb)
                    ON CONFLICT (id_empresa,id_filial,id_entidade)
                    DO UPDATE SET payload=EXCLUDED.payload, ingested_at=now()
                    """,
                    clientes,
                )

                cur.executemany(
                    """
                    INSERT INTO stg.movprodutos (id_empresa,id_filial,id_db,id_movprodutos,payload)
                    VALUES (%s,%s,%s,%s,%s::jsonb)
                    ON CONFLICT (id_empresa,id_filial,id_db,id_movprodutos)
                    DO UPDATE SET payload=EXCLUDED.payload, ingested_at=now()
                    """,
                    mov_rows,
                )

                cur.executemany(
                    """
                    INSERT INTO stg.itensmovprodutos (id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos,payload)
                    VALUES (%s,%s,%s,%s,%s,%s::jsonb)
                    ON CONFLICT (id_empresa,id_filial,id_db,id_movprodutos,id_itensmovprodutos)
                    DO UPDATE SET payload=EXCLUDED.payload, ingested_at=now()
                    """,
                    item_rows,
                )

                cur.executemany(
                    """
                    INSERT INTO stg.comprovantes (id_empresa,id_filial,id_db,id_comprovante,payload)
                    VALUES (%s,%s,%s,%s,%s::jsonb)
                    ON CONFLICT (id_empresa,id_filial,id_db,id_comprovante)
                    DO UPDATE SET payload=EXCLUDED.payload, ingested_at=now()
                    """,
                    comp_rows,
                )

        result = conn.execute("SELECT etl.run_all(%s, %s) AS result", (id_empresa, True)).fetchone()["result"]

    print("\n=== Demo load concluído ===")
    print(f"Movimentos: {len(mov_rows)}")
    print(f"Itens:      {len(item_rows)}")
    print(f"Comprov.:   {len(comp_rows)}")
    print("ETL:", result)


if __name__ == "__main__":
    main()
