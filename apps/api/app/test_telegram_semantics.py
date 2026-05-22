from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app.services.telegram import notify_cancelled_comprovantes, notify_voided_nfes, raw_comprovante_is_cancelled, raw_nfe_is_voided


class TelegramSemanticsTests(unittest.TestCase):
    def test_raw_comprovante_is_cancelled_only_cancelado_true(self) -> None:
        # cancelado=true is a real cancellation
        self.assertTrue(raw_comprovante_is_cancelled({"CANCELADO": True, "SITUACAO": 1}))
        self.assertTrue(raw_comprovante_is_cancelled({"CANCELADO": True, "SITUACAO": 3}))
        self.assertTrue(raw_comprovante_is_cancelled({"CANCELADO": True}))
        # cancelado=false is NOT cancelled, regardless of situacao
        self.assertFalse(raw_comprovante_is_cancelled({"CANCELADO": False, "SITUACAO": 2}))
        self.assertFalse(raw_comprovante_is_cancelled({"CANCELADO": False}))
        # situacao=2 alone is NOT treated as cancellation anymore
        self.assertFalse(raw_comprovante_is_cancelled({"SITUACAO": 2}))
        self.assertFalse(raw_comprovante_is_cancelled({"SITUACAO": 3}))

    def test_raw_nfe_is_voided_detects_status_5(self) -> None:
        self.assertTrue(raw_nfe_is_voided({"STATUS": 5}))
        self.assertTrue(raw_nfe_is_voided({"STATUSNFE": 5}))
        self.assertTrue(raw_nfe_is_voided({"status": "5"}))
        self.assertFalse(raw_nfe_is_voided({"STATUS": 1}))
        self.assertFalse(raw_nfe_is_voided({"STATUS": 2}))
        self.assertFalse(raw_nfe_is_voided({}))

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
                    with patch("app.services.telegram._resolve_filial_nome", return_value="VR01"):
                        with patch("app.services.telegram._send_telegram", new_callable=AsyncMock) as send_mock:
                            asyncio.run(notify_cancelled_comprovantes(id_empresa=1, raw_rows=[row]))

        send_mock.assert_awaited_once()
        msg = send_mock.call_args[0][1]
        self.assertIn("VENDA CANCELADA na filial VR01", msg)
        self.assertIn("9002", msg)

    def test_notify_cancelled_comprovantes_ignores_situacao_2(self) -> None:
        """situacao=2 without cancelado=true should NOT trigger notification."""
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
                with patch("app.services.telegram._send_telegram", new_callable=AsyncMock) as send_mock:
                    asyncio.run(notify_cancelled_comprovantes(id_empresa=1, raw_rows=[row]))

        send_mock.assert_not_awaited()

    def test_notify_voided_nfes_triggers_on_status_5(self) -> None:
        row = {
            "ID_FILIAL": 1,
            "ID_DB": 1,
            "ID_NFE": 500,
            "NRONF": "123456",
            "SERIE": "1",
            "STATUS": 5,
            "VALOR": 150.0,
            "DATA": "2026-04-01",
            "DATAINUTILIZACAO": "2026-04-02",
            "ID_USUARIOS": 7,
        }

        with patch("app.services.telegram.settings.telegram_bot_token", "token"):
            with patch("app.services.telegram._get_recipients", return_value=["chat-1"]):
                with patch("app.services.telegram._register_dispatch_once", return_value=True):
                    with patch("app.services.telegram._resolve_filial_nome", return_value="VR02"):
                        with patch("app.services.telegram._send_telegram", new_callable=AsyncMock) as send_mock:
                            asyncio.run(notify_voided_nfes(id_empresa=1, raw_rows=[row]))

        send_mock.assert_awaited_once()
        msg = send_mock.call_args[0][1]
        self.assertIn("NOTA INUTILIZADA na filial VR02", msg)
        self.assertIn("123456", msg)


if __name__ == "__main__":
    unittest.main()
