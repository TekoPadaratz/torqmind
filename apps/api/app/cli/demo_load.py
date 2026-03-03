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
    id_db = 1
    filiais_ids = [1, 2, 3]

    # --- Dimensions
    filiais = []
    for id_filial in filiais_ids:
        filiais.append(
            (
                id_empresa,
                id_filial,
                _j(
                    {
                        "ID_FILIAL": id_filial,
                        "NOMEFILIAL": f"Filial Demo {id_filial}",
                        "CNPJ": f"00.000.000/000{id_filial}-00",
                    }
                ),
            )
        )

    grupos = []
    for id_filial in filiais_ids:
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
    for id_filial in filiais_ids:
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
    for id_filial in filiais_ids:
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
    for id_filial in filiais_ids:
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
    for id_filial in filiais_ids:
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
    risky_user = 5
    risky_employee = 7

    movement_idx = 0
    for d in range(14):
        day = start + timedelta(days=d)
        for filial_idx, id_filial in enumerate(filiais_ids):
            movements_in_filial = 27 if filial_idx < 2 else 26
            for _ in range(movements_in_filial):
                movement_idx += 1
                is_risky_context = (id_filial == 1) and (random.random() < 0.22)
                hour = random.randint(18, 22) if is_risky_context else random.randint(6, 21)
                ts = day.replace(hour=hour, minute=random.randint(0, 59), second=0, microsecond=0)

                id_mov += 1
                id_comp += 1

                id_turno = random.randint(1, 3)
                id_usuario = risky_user if is_risky_context else random.randint(1, 5)
                id_cliente = random.randint(1, 40)
                id_funcionario = risky_employee if is_risky_context else random.randint(1, 7)

                # 1120 movimentos / 2804 itens:
                # base 2 itens por movimento + 564 movimentos com 3 itens.
                # deterministic and reproducible.
                n_itens = 3 if movement_idx <= 564 else 2
                total_venda = 0.0

                for _i in range(n_itens):
                    id_item += 1
                    id_prod = random.randint(1, 20)
                    qtd = random.randint(1, 3)
                    preco = round(random.uniform(5.0, 40.0), 2)
                    desconto = 0.0
                    if is_risky_context and random.random() < 0.35:
                        # desconto fora da curva para o usuário/funcionário suspeito
                        desconto = round(preco * random.uniform(0.18, 0.38), 2)
                    elif random.random() < 0.05:
                        desconto = round(preco * random.uniform(0.05, 0.12), 2)
                    preco_final = max(0.1, preco - desconto)
                    total = round(qtd * preco_final, 2)
                    total_venda += total

                    item_payload = {
                        "ID_ITENSMOVPRODUTOS": id_item,
                        "ID_MOVPRODUTOS": id_mov,
                        "ID_FILIAL": id_filial,
                        "ID_DB": id_db,
                        "ID_PRODUTOS": id_prod,
                        "QTDE": qtd,
                        "VLRUNITARIO": preco_final,
                        "TOTAL": total,
                        "VLRDESCONTO": round(desconto * qtd, 2),
                        # regra do seu mapping: venda boa = CFOP > 4999
                        "CFOP": 5102,
                        "ID_FUNCIONARIOS": id_funcionario,
                    }

                    item_rows.append((id_empresa, id_filial, id_db, id_mov, id_item, _j(item_payload)))

                cancelado = random.random() < (0.18 if is_risky_context else 0.04)
                if is_risky_context and random.random() < 0.20:
                    # força alguns cancelamentos com alto valor para o motor de risco
                    total_venda = round(total_venda * random.uniform(2.0, 3.2), 2)
                total_venda = round(total_venda, 2)

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

        result = conn.execute(
            "SELECT etl.run_all(%s, %s, %s) AS result",
            (id_empresa, True, True),
        ).fetchone()["result"]
        conn.commit()

    print("\n=== Demo load concluído ===")
    print(f"Movimentos: {len(mov_rows)}")
    print(f"Itens:      {len(item_rows)}")
    print(f"Comprov.:   {len(comp_rows)}")
    print("ETL:", result)


if __name__ == "__main__":
    main()
