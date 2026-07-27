"""Unit: sinal/filtro do ajuste de plano bancário (sem espelho em MOVBANCOS)."""

from __future__ import annotations

import unittest
from decimal import Decimal


def movbancos_ajuste_plano_sinal(payload: dict) -> Decimal:
    """Espelho de etl.movbancos_ajuste_plano_sinal (migration 126)."""
    valor = Decimal(str(payload.get("VALOR") or 0))
    tipo = int(round(float(payload.get("TIPO")))) if payload.get("TIPO") is not None else -1
    if tipo == 0:
        return valor
    if tipo == 1:
        return -valor
    return Decimal("0")


def movbancos_ajuste_plano_documento_ok(documento: str | None) -> bool:
    """Espelho de etl.movbancos_ajuste_plano_documento_ok (migration 127)."""
    d = (documento or "").strip().upper()
    return (
        d.startswith("TRANSF AJUSTE")
        or d.startswith("AJUSTE-SALDO")
        or d.startswith("AJUSTE SALDO")
        or d.startswith("AJUSTE DE SALDOS")
        or d.startswith("AJUSTE EMPRESTIMO")
    )


class MovbancosAjustePlanoSinalTest(unittest.TestCase):
    def test_banrisul_transf_ajuste_pix_bate_alvo(self) -> None:
        movbancos_cum = Decimal("152833.35")
        ajuste = movbancos_ajuste_plano_sinal(
            {"TIPO": 1, "VALOR": "24869.45", "DOCUMENTO": "TRANSF AJUSTE PIX "}
        )
        self.assertEqual(ajuste, Decimal("-24869.45"))
        self.assertEqual(movbancos_cum + ajuste, Decimal("127963.90"))

    def test_contrapartida_sicoob_credito(self) -> None:
        ajuste = movbancos_ajuste_plano_sinal(
            {"TIPO": 0, "VALOR": "24869.45", "DOCUMENTO": "TRANSF AJUSTE PIX "}
        )
        self.assertEqual(ajuste, Decimal("24869.45"))

    def test_tipo_desconhecido_zero(self) -> None:
        self.assertEqual(
            movbancos_ajuste_plano_sinal({"TIPO": 2, "VALOR": "10"}),
            Decimal("0"),
        )

    def test_documento_ok_padroes(self) -> None:
        self.assertTrue(movbancos_ajuste_plano_documento_ok("TRANSF AJUSTE PIX "))
        self.assertTrue(movbancos_ajuste_plano_documento_ok("AJUSTE-SALDO CREDOR VR05"))
        self.assertTrue(movbancos_ajuste_plano_documento_ok("AJUSTE SALDO"))
        self.assertTrue(movbancos_ajuste_plano_documento_ok("Ajuste de Saldos"))
        self.assertTrue(movbancos_ajuste_plano_documento_ok("AJUSTE EMPRESTIMO VR05"))
        self.assertTrue(movbancos_ajuste_plano_documento_ok("ajuste saldo TVR"))
        self.assertFalse(movbancos_ajuste_plano_documento_ok("ajuste a taxa antecipacao visa"))
        self.assertFalse(movbancos_ajuste_plano_documento_ok("TRANSF VR01 P/TVR"))


if __name__ == "__main__":
    unittest.main()
