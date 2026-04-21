from __future__ import annotations

import json
import unittest
from datetime import datetime

from fastapi.testclient import TestClient

from app import business_time, routes_ingest
from app.config import settings
from app.db import get_conn
from app.main import app


class IngestTimeParsingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def setUp(self) -> None:
        self._orig_business_timezone = settings.business_timezone
        self._orig_business_tenant_timezones = settings.business_tenant_timezones
        business_time._tenant_timezone_map.cache_clear()
        business_time._zoneinfo.cache_clear()

    def tearDown(self) -> None:
        settings.business_timezone = self._orig_business_timezone
        settings.business_tenant_timezones = self._orig_business_tenant_timezones
        business_time._tenant_timezone_map.cache_clear()
        business_time._zoneinfo.cache_clear()

    def _create_tenant(self, name: str) -> tuple[int, str]:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(
                """
                INSERT INTO app.tenants (
                  nome,
                  is_active,
                  status,
                  billing_status,
                  valid_from
                )
                VALUES (%s, true, 'active', 'current', CURRENT_DATE)
                RETURNING id_empresa, ingest_key::text
                """,
                (name,),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO auth.filiais (id_empresa, id_filial, nome, is_active, valid_from)
                VALUES (%s, 1, %s, true, CURRENT_DATE)
                ON CONFLICT (id_empresa, id_filial)
                DO UPDATE SET nome = EXCLUDED.nome, is_active = EXCLUDED.is_active
                """,
                (row["id_empresa"], f"Filial {name}"),
            )
            conn.commit()
        return int(row["id_empresa"]), str(row["ingest_key"])

    def _post_ndjson(self, dataset: str, ingest_key: str, rows: list[dict[str, object]]) -> dict[str, object]:
        payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
        response = self.client.post(
            f"/ingest/{dataset}",
            data=payload.encode("utf-8"),
            headers={
                "X-Ingest-Key": ingest_key,
                "Content-Type": "application/x-ndjson",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def test_parse_ts_string_iso_without_timezone_uses_business_timezone(self) -> None:
        settings.business_timezone = "America/Sao_Paulo"
        parsed = routes_ingest._parse_ts("2026-03-30T00:30:00")

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.isoformat(), "2026-03-30T00:30:00-03:00")

    def test_parse_ts_python_datetime_without_tzinfo_uses_business_timezone(self) -> None:
        settings.business_timezone = "America/Sao_Paulo"
        parsed = routes_ingest._parse_ts(datetime(2026, 3, 30, 0, 30, 0))

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.isoformat(), "2026-03-30T00:30:00-03:00")

    def test_parse_ts_preserves_utc_z_payload(self) -> None:
        parsed = routes_ingest._parse_ts("2026-03-30T03:30:00Z")

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.isoformat(), "2026-03-30T03:30:00+00:00")

    def test_parse_ts_preserves_explicit_negative_offset(self) -> None:
        parsed = routes_ingest._parse_ts("2026-03-30T00:30:00-03:00")

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.isoformat(), "2026-03-30T00:30:00-03:00")

    def test_infer_dt_evento_keeps_business_semantics_for_naive_payload(self) -> None:
        settings.business_timezone = "America/Sao_Paulo"
        parsed = routes_ingest._infer_dt_evento({"DATA": "2026-03-30T00:30:00"})

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.isoformat(), "2026-03-30T00:30:00-03:00")
        self.assertEqual(business_time.business_date_for_datetime(parsed, tenant_id=1).isoformat(), "2026-03-30")

    def test_ingest_persists_naive_operational_timestamp_with_business_date(self) -> None:
        settings.business_timezone = "America/Sao_Paulo"
        tenant_id, ingest_key = self._create_tenant("Tenant Ingest Naive Timestamp")

        result = self._post_ndjson(
            "comprovantes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 9901,
                    "ID_USUARIOS": 1,
                    "ID_TURNOS": 1,
                    "ID_ENTIDADE": 10,
                    "VLRTOTAL": 123.45,
                    "REFERENCIA": 88001,
                    "DATA": "2026-03-30T00:30:00",
                }
            ],
        )

        self.assertTrue(bool(result["ok"]))
        self.assertEqual(int(result["inserted"]), 1)
        self.assertEqual(int(result["updated"]), 0)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(
                """
                SELECT
                  dt_evento,
                  dt_evento AT TIME ZONE %s AS dt_evento_local
                FROM stg.comprovantes
                WHERE id_empresa = %s
                  AND id_filial = 1
                  AND id_db = 1
                  AND id_comprovante = 9901
                """,
                (business_time.business_timezone_name(tenant_id), tenant_id),
            ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row["dt_evento_local"].isoformat(), "2026-03-30T00:30:00")
        self.assertEqual(business_time.business_date_for_datetime(row["dt_evento"], tenant_id=tenant_id).isoformat(), "2026-03-30")

    def test_ingest_preserves_explicit_offsets_near_midnight_without_shifting_business_day(self) -> None:
        settings.business_timezone = "America/Sao_Paulo"
        tenant_id, ingest_key = self._create_tenant("Tenant Ingest Explicit Offset Boundary")
        cases = [
            (9911, "2026-03-31T21:30:00-03:00", "2026-03-31T21:30:00", "2026-03-31"),
            (9912, "2026-03-31T22:33:00-03:00", "2026-03-31T22:33:00", "2026-03-31"),
            (9913, "2026-03-31T23:59:00-03:00", "2026-03-31T23:59:00", "2026-03-31"),
            (9914, "2026-04-01T00:10:00-03:00", "2026-04-01T00:10:00", "2026-04-01"),
        ]

        rows = [
            {
                "ID_FILIAL": 1,
                "ID_DB": 1,
                "ID_COMPROVANTE": id_comprovante,
                "ID_USUARIOS": 1,
                "ID_TURNOS": 1,
                "ID_ENTIDADE": 10,
                "VLRTOTAL": 50,
                "REFERENCIA": 88000 + idx,
                "DATA": event_ts,
            }
            for idx, (id_comprovante, event_ts, _, _) in enumerate(cases, start=1)
        ]

        result = self._post_ndjson("comprovantes", ingest_key, rows)
        self.assertTrue(bool(result["ok"]))
        self.assertEqual(int(result["inserted"]), len(cases))
        self.assertEqual(int(result["updated"]), 0)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            stored_rows = conn.execute(
                """
                SELECT
                  id_comprovante,
                  dt_evento,
                  dt_evento AT TIME ZONE %s AS dt_evento_local
                FROM stg.comprovantes
                WHERE id_empresa = %s
                  AND id_filial = 1
                  AND id_db = 1
                  AND id_comprovante = ANY(%s)
                ORDER BY id_comprovante
                """,
                (
                    business_time.business_timezone_name(tenant_id),
                    tenant_id,
                    [id_comprovante for id_comprovante, _, _, _ in cases],
                ),
            ).fetchall()

        self.assertEqual(len(stored_rows), len(cases))
        by_id = {int(row["id_comprovante"]): row for row in stored_rows}

        for id_comprovante, _, expected_local, expected_business_date in cases:
            row = by_id[id_comprovante]
            self.assertEqual(row["dt_evento_local"].isoformat(), expected_local)
            self.assertEqual(
                business_time.business_date_for_datetime(row["dt_evento"], tenant_id=tenant_id).isoformat(),
                expected_business_date,
            )
