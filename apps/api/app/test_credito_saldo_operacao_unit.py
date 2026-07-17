"""Unit: reconstrução de saldo na operação (créditos antifraude)."""

from __future__ import annotations

import unittest


def reconstruct_saldo_apos(
    saldo_atual: float,
    movs_asc: list[dict],
) -> dict[int, float]:
    """Espelha a lógica de fraud_lancamentos_creditos (do mais novo ao mais antigo)."""
    running = float(saldo_atual)
    out: dict[int, float] = {}
    for lr in reversed(movs_asc):
        id_mov = int(lr["id_mov"])
        running = round(running, 2)
        out[id_mov] = running
        running = round(running - float(lr.get("entradas") or 0) + float(lr.get("saidas") or 0), 2)
    return out


class CreditoSaldoOperacaoUnitTest(unittest.TestCase):
    def test_mesmo_dia_tres_injecoes_ordem_por_id_mov(self) -> None:
        # Exemplo do usuário: 250, 950, 9900 no mesmo dia; depois usos/injeções.
        # Ledger ASC por ID_MOV. Saldo atual = 21209.
        movs = [
            {"id_mov": 100, "entradas": 250.0, "saidas": 0.0},
            {"id_mov": 101, "entradas": 950.0, "saidas": 0.0},
            {"id_mov": 102, "entradas": 9900.0, "saidas": 0.0},
            {"id_mov": 200, "entradas": 0.0, "saidas": 5000.0},
            {"id_mov": 201, "entradas": 10000.0, "saidas": 0.0},
            {"id_mov": 202, "entradas": 0.0, "saidas": 4891.0},
        ]
        # net = +250+950+9900 -5000 +10000 -4891 = 11209... wait need consistent atual
        # Force: after all movs, saldo = 21209
        # Opening before 100: X
        # After walk: X+250+950+9900-5000+10000-4891 = 21209
        # X + 11209 = 21209 => X = 10000
        saldo_atual = 21209.0
        pos = reconstruct_saldo_apos(saldo_atual, movs)
        self.assertEqual(pos[202], 21209.0)
        self.assertEqual(pos[201], round(21209 + 4891, 2))  # antes da última saída
        self.assertEqual(pos[200], round(pos[201] - 10000, 2))
        self.assertEqual(pos[102], round(pos[200] + 5000, 2))
        self.assertEqual(pos[101], round(pos[102] - 9900, 2))
        self.assertEqual(pos[100], round(pos[101] - 950, 2))
        # Cada injeção do dia fica com saldo distinto
        self.assertNotEqual(pos[100], pos[101])
        self.assertNotEqual(pos[101], pos[102])


if __name__ == "__main__":
    unittest.main()
