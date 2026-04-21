from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app.services.telegram import notify_cancelled_comprovantes, raw_comprovante_is_cancelled


class TelegramSemanticsTests(unittest.TestCase):
    def test_raw_comprovante_is_cancelled_prioritizes_cancelado_flag_when_present(self) -> None:
        self.assertFalse(raw_comprovante_is_cancelled({"CANCELADO": False, "SITUACAO": 2}))
        self.assertTrue(raw_comprovante_is_cancelled({"CANCELADO": True, "SITUACAO": 3}))
        self.assertTrue(raw_comprovante_is_cancelled({"CANCELADO": True, "SITUACAO": 5}))
        self.assertTrue(raw_comprovante_is_cancelled({"CANCELADO": True}))
        self.assertFalse(raw_comprovante_is_cancelled({"CANCELADO": False}))

    def test_notify_cancelled_comprovantes_triggers_on_cancelado_true(self) -> None:
        row = {
            "ID_FILIAL": 1,
            "ID_DB": 1,
            "ID_COMPROVANTE": 9002,
            "ID_USUARIOS": 12,
            "ID_TURNOS": 4,
            "VLRTOTAL": 40,
            "CANCELADO": True,
            "SITUACAO": 1,
            "DATA": "2026-03-31 11:00:00",
        }

        with patch("app.services.telegram.settings.telegram_bot_token", "token"):
            with patch("app.services.telegram._get_recipients", return_value=["chat-1"]):
                with patch("app.services.telegram._insert_alert_if_new", return_value=True):
                    with patch("app.services.telegram._send_telegram", new_callable=AsyncMock) as send_mock:
                        asyncio.run(notify_cancelled_comprovantes(id_empresa=1, raw_rows=[row]))

        send_mock.assert_awaited_once()

    def test_notify_cancelled_comprovantes_keeps_legacy_fallback_when_cancelado_absent(self) -> None:
        row = {
            "ID_FILIAL": 1,
            "ID_DB": 1,
            "ID_COMPROVANTE": 9001,
            "ID_USUARIOS": 11,
            "ID_TURNOS": 3,
            "VLRTOTAL": 60,
            "SITUACAO": 2,
            "DATA": "2026-03-31 10:00:00",
        }

        with patch("app.services.telegram.settings.telegram_bot_token", "token"):
            with patch("app.services.telegram._get_recipients", return_value=["chat-1"]):
                with patch("app.services.telegram._insert_alert_if_new", return_value=True):
                    with patch("app.services.telegram._send_telegram", new_callable=AsyncMock) as send_mock:
                        asyncio.run(notify_cancelled_comprovantes(id_empresa=1, raw_rows=[row]))

        send_mock.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
