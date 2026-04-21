from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone

from app import business_time
from app.config import settings


class BusinessTimeTest(unittest.TestCase):
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

    def test_business_timezone_prefers_tenant_override(self) -> None:
        settings.business_timezone = "America/Sao_Paulo"
        settings.business_tenant_timezones = '{"7":"UTC","11":"America/Manaus"}'
        business_time._tenant_timezone_map.cache_clear()
        business_time._zoneinfo.cache_clear()

        self.assertEqual(business_time.business_timezone_name(7), "UTC")
        self.assertEqual(business_time.business_timezone_name(11), "America/Manaus")
        self.assertEqual(business_time.business_timezone_name(99), "America/Sao_Paulo")

    def test_resolve_business_date_keeps_explicit_reference(self) -> None:
        explicit = business_time.resolve_business_date(date(2026, 3, 30), tenant_id=1)
        self.assertEqual(explicit.isoformat(), "2026-03-30")

    def test_business_date_for_datetime_uses_tenant_timezone(self) -> None:
        settings.business_timezone = "America/Sao_Paulo"
        settings.business_tenant_timezones = '{"9":"America/Manaus"}'
        business_time._tenant_timezone_map.cache_clear()
        business_time._zoneinfo.cache_clear()

        event_at = datetime(2026, 3, 30, 2, 30, tzinfo=timezone.utc)
        self.assertEqual(business_time.business_date_for_datetime(event_at, tenant_id=9).isoformat(), "2026-03-29")
        self.assertEqual(business_time.business_date_for_datetime(event_at, tenant_id=1).isoformat(), "2026-03-29")

    def test_business_date_for_datetime_keeps_sao_paulo_night_boundary_on_31_03_and_01_04(self) -> None:
        settings.business_timezone = "America/Sao_Paulo"
        business_time._tenant_timezone_map.cache_clear()
        business_time._zoneinfo.cache_clear()

        sao_paulo = timezone(timedelta(hours=-3))
        cases = [
            (datetime(2026, 3, 31, 21, 30, tzinfo=sao_paulo), "2026-03-31"),
            (datetime(2026, 3, 31, 22, 33, tzinfo=sao_paulo), "2026-03-31"),
            (datetime(2026, 3, 31, 23, 59, tzinfo=sao_paulo), "2026-03-31"),
            (datetime(2026, 4, 1, 0, 10, tzinfo=sao_paulo), "2026-04-01"),
        ]

        for event_at, expected_date in cases:
            self.assertEqual(
                business_time.business_date_for_datetime(event_at, tenant_id=1).isoformat(),
                expected_date,
            )
            self.assertEqual(
                business_time.business_date_for_datetime(event_at.astimezone(timezone.utc), tenant_id=1).isoformat(),
                expected_date,
            )
