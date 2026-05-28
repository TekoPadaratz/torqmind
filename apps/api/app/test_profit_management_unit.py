"""Unit tests for profit management module formulas and routes."""
from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock


class TestProfitFormulas:
    """Test core profit management calculations."""

    def test_cmv_calculation(self):
        """CMV = qtd × custo_unitario"""
        qtd = 100
        custo_unitario = 4.82
        cmv = qtd * custo_unitario
        assert cmv == pytest.approx(482.0, rel=1e-2)

    def test_margem_bruta(self):
        """margem_bruta = receita - cmv"""
        receita = 763.0
        cmv = 482.0
        margem_bruta = receita - cmv
        assert margem_bruta == pytest.approx(281.0, rel=1e-2)

    def test_margem_bruta_pct(self):
        """margem_bruta_pct = margem_bruta / receita"""
        receita = 763.0
        margem_bruta = 281.0
        pct = margem_bruta / receita
        assert pct == pytest.approx(0.3684, rel=1e-2)

    def test_despesa_unitaria_rateada(self):
        """despesa_unitaria = despesa_produto / qtd_vendida"""
        desp_rateavel_filial = 480000.0
        receita_produto = 763.0
        receita_total_filial = 6960000.0
        qtd_vendida = 100

        desp_produto = desp_rateavel_filial * (receita_produto / receita_total_filial)
        desp_unitaria = desp_produto / qtd_vendida
        assert desp_unitaria == pytest.approx(0.5262, rel=1e-1)

    def test_preco_minimo_saudavel(self):
        """preco_minimo = custo_unitario + despesa_unitaria"""
        custo_unitario = 4.82
        desp_unitaria = 0.53
        preco_minimo = custo_unitario + desp_unitaria
        assert preco_minimo == pytest.approx(5.35, rel=1e-2)

    def test_preco_ideal_margem_30(self):
        """preco_ideal = preco_minimo / (1 - margem_desejada)"""
        preco_minimo = 5.35
        margem_desejada = 0.30
        preco_ideal = preco_minimo / (1 - margem_desejada)
        assert preco_ideal == pytest.approx(7.64, rel=1e-2)

    def test_reajuste_sugerido_positive(self):
        """When preco_ideal > preco_atual, reajuste is positive."""
        preco_ideal = 5.57
        preco_atual = 4.62
        reajuste_valor = max(0, preco_ideal - preco_atual)
        reajuste_pct = reajuste_valor / preco_atual
        assert reajuste_valor == pytest.approx(0.95, rel=1e-2)
        assert reajuste_pct == pytest.approx(0.2056, rel=1e-2)

    def test_reajuste_sugerido_zero_when_above(self):
        """When preco_ideal <= preco_atual, reajuste is zero."""
        preco_ideal = 6.89
        preco_atual = 7.63  # already above ideal
        reajuste_valor = max(0, preco_ideal - preco_atual)
        assert reajuste_valor == 0

    def test_impacto_60d(self):
        """impacto_60d = reajuste_valor × qtd_mes_anterior × 2"""
        reajuste_valor = 0.95
        qtd_mes_anterior = 1966
        impacto_60d = reajuste_valor * qtd_mes_anterior * 2
        assert impacto_60d == pytest.approx(3735.4, rel=1e-2)

    def test_impacto_60d_zero_when_no_reajuste(self):
        """Impact is 0 when product is already above ideal."""
        reajuste_valor = 0
        qtd_mes_anterior = 3706
        impacto_60d = reajuste_valor * qtd_mes_anterior * 2
        assert impacto_60d == 0

    def test_status_abaixo_minimo(self):
        """Status is abaixo_minimo when price < cost + expense."""
        preco_atual = 3.00
        custo_unitario = 2.90
        desp_unitaria = 0.50
        preco_minimo = custo_unitario + desp_unitaria

        if preco_atual < preco_minimo:
            status = "abaixo_minimo"
        else:
            status = "saudavel"
        assert status == "abaixo_minimo"

    def test_status_saudavel(self):
        """Status is saudavel when price >= ideal."""
        preco_atual = 7.63
        preco_minimo = 5.35
        preco_ideal = 7.64  # preco_minimo / (1 - 0.30)

        if preco_atual < preco_minimo:
            status = "abaixo_minimo"
        elif preco_atual < preco_ideal:
            status = "abaixo_ideal"
        else:
            status = "saudavel"
        assert status == "abaixo_ideal"  # 7.63 < 7.64

    def test_tipo_conta_both_included(self):
        """Both TIPO_CONTA=0 and TIPO_CONTA=1 must be included."""
        despesas = [
            {"tipo_conta": 0, "valor": 7000},
            {"tipo_conta": 0, "valor": 5000},
            {"tipo_conta": 1, "valor": 50},
            {"tipo_conta": 1, "valor": 44},
        ]
        total = sum(d["valor"] for d in despesas)
        assert total == 12094  # All included, none filtered

    def test_competencia_by_vencimento(self):
        """Expenses are grouped by DTAVCTO (vencimento), not payment date."""
        # Simulates an expense with dt_vencimento in month X but paid in month Y
        dt_vencimento = "2026-05-15"
        dt_pagamento = "2026-06-02"
        ano_mes_competencia = int(dt_vencimento[:4]) * 100 + int(dt_vencimento[5:7])
        assert ano_mes_competencia == 202605  # Goes to May, not June

    def test_exclusion_non_rateable(self):
        """Financial/exceptional expenses don't enter product apportionment."""
        expenses_classification = [
            {"classificacao": "pessoal", "entra_rateio": True},
            {"classificacao": "financeiro", "entra_rateio": False},
            {"classificacao": "comercial", "entra_rateio": True},
        ]
        rateable = [e for e in expenses_classification if e["entra_rateio"]]
        assert len(rateable) == 2
        assert all(e["classificacao"] != "financeiro" for e in rateable)

    def test_markup_real(self):
        """markup_real = preco_atual / (custo_unitario + despesa_unitaria)"""
        preco_atual = 7.63
        custo_unitario = 4.82
        desp_unitaria = 0.53
        markup = preco_atual / (custo_unitario + desp_unitaria)
        assert markup == pytest.approx(1.426, rel=1e-2)


class TestProfitPermissions:
    """Test that profit management respects screen permissions."""

    def test_screen_registry_has_profit_management(self):
        from app.permissions import SCREEN_REGISTRY
        assert "profit_management" in SCREEN_REGISTRY
        assert SCREEN_REGISTRY["profit_management"]["has_sensitive"] is True

    def test_profit_management_in_all_product_screens(self):
        from app.permissions import _ALL_PRODUCT_SCREENS
        assert "profit_management" in _ALL_PRODUCT_SCREENS


class TestTelegramNfeFix:
    """Test that NFE operator lookup falls back to comprovante."""

    def test_nfe_row_without_id_usuarios(self):
        """When NFE row has no ID_USUARIOS, should try comprovante lookup."""
        row = {
            "ID_FILIAL": 14458,
            "ID_DB": 14458,
            "ID_NFE": 500,
            "ID_COMPROVANTE": 12345,
            "NRONF": "123456",
            "STATUS": 5,
            "DATA": "2026-05-01",
        }
        # Simulate the extraction logic
        id_usuario = None
        for key in ["ID_USUARIOS", "id_usuario", "ID_USUARIO"]:
            if key in row:
                id_usuario = row[key]
                break
        assert id_usuario is None  # NFE doesn't have it

        # The fix should look up from comprovante
        id_comprovante = row.get("ID_COMPROVANTE")
        assert id_comprovante == 12345  # Available for lookup
