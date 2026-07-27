"""Ingest upsert must not rewrite identical payloads (SSD / WAL / CDC)."""
from __future__ import annotations

import json
import unittest

from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app


class IngestNoopUpsertTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def _create_tenant(self, name: str) -> tuple[int, str]:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(
                """
                INSERT INTO app.tenants (
                  nome, is_active, status, billing_status, valid_from,
                  sales_history_days, default_product_scope_days
                )
                VALUES (%s, true, 'active', 'current', CURRENT_DATE, 365, 1)
                RETURNING id_empresa, ingest_key::text
                """,
                (name,),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO auth.filiais (id_empresa, id_filial, nome, is_active, valid_from)
                VALUES (%s, 1, %s, true, CURRENT_DATE)
                ON CONFLICT (id_empresa, id_filial) DO UPDATE SET nome = EXCLUDED.nome
                """,
                (row["id_empresa"], f"Filial {name}"),
            )
            conn.commit()
        return int(row["id_empresa"]), str(row["ingest_key"])

    def _post(self, dataset: str, ingest_key: str, rows: list[dict]) -> dict:
        payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
        response = self.client.post(
            f"/ingest/{dataset}",
            data=payload.encode("utf-8"),
            headers={"X-Ingest-Key": ingest_key, "Content-Type": "application/x-ndjson"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def test_identical_payload_counts_as_unchanged_and_keeps_timestamps(self) -> None:
        tenant_id, ingest_key = self._create_tenant("Tenant Noop Upsert")
        row = {
            "ID_FILIAL": 1,
            "ID_USUARIOS": 9001,
            "ID_USUARIO": 9001,
            "NOME": "Operador Noop",
            "ATIVO": True,
        }
        first = self._post("usuarios", ingest_key, [row])
        self.assertEqual(int(first["inserted"]), 1)
        self.assertEqual(int(first["updated"]), 0)
        self.assertEqual(int(first.get("unchanged") or 0), 0)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            before = conn.execute(
                """
                SELECT ingested_at, received_at, payload
                FROM stg.usuarios
                WHERE id_empresa = %s AND id_filial = 1 AND id_usuario = 9001
                """,
                (tenant_id,),
            ).fetchone()

        second = self._post("usuarios", ingest_key, [row])
        self.assertEqual(int(second["inserted"]), 0)
        self.assertEqual(int(second["updated"]), 0)
        self.assertEqual(int(second.get("unchanged") or 0), 1)
        self.assertEqual(int(second["inserted_or_updated"]), 0)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            after = conn.execute(
                """
                SELECT ingested_at, received_at, payload
                FROM stg.usuarios
                WHERE id_empresa = %s AND id_filial = 1 AND id_usuario = 9001
                """,
                (tenant_id,),
            ).fetchone()

        self.assertEqual(before["ingested_at"], after["ingested_at"])
        self.assertEqual(before["received_at"], after["received_at"])

        changed = dict(row)
        changed["NOME"] = "Operador Atualizado"
        third = self._post("usuarios", ingest_key, [changed])
        self.assertEqual(int(third["inserted"]), 0)
        self.assertEqual(int(third["updated"]), 1)
        self.assertEqual(int(third.get("unchanged") or 0), 0)
        self.assertEqual(int(third["inserted_or_updated"]), 1)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            final = conn.execute(
                """
                SELECT ingested_at, received_at, payload->>'NOME' AS nome
                FROM stg.usuarios
                WHERE id_empresa = %s AND id_filial = 1 AND id_usuario = 9001
                """,
                (tenant_id,),
            ).fetchone()

        self.assertEqual(final["nome"], "Operador Atualizado")
        self.assertGreater(final["ingested_at"], before["ingested_at"])
        self.assertGreater(final["received_at"], before["received_at"])


if __name__ == "__main__":
    unittest.main()
