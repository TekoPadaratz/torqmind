"""Unit: mapeamento CODIGOPLANODECONTAS Xpert → buckets DRE."""

from __future__ import annotations


def _classificar(codigo: str) -> str:
    """Espelho das regras de etl.load_dim_plano_contas_gerencial (migration 114)."""
    c = codigo or ""
    if c.startswith("3.2.01"):
        return "pessoal"
    if c.startswith("3.2.02") or c.startswith("3.2.08"):
        return "comercial"
    if c.startswith("3.2.03") or c.startswith("3.2.07"):
        return "administrativo"
    if c.startswith("3.2.04"):
        return "financeiro"
    if c.startswith("3.2.05"):
        return "tributos"
    if c.startswith("3.2.09") or c.startswith("3.3"):
        return "excepcional"
    if c.startswith("3.2"):
        return "nao_classificado"
    return "outros"


def _entra_dre(codigo: str) -> bool:
    c = codigo or ""
    return c.startswith("3.2") or c.startswith("3.3")


class TestPlanoContasDreBuckets:
    def test_funcionarios_pessoal(self):
        assert _classificar("3.2.01.02") == "pessoal"
        assert _entra_dre("3.2.01.02")

    def test_taxas_cartao_comercial(self):
        assert _classificar("3.2.02.07") == "comercial"
        assert _classificar("3.2.02.08.01") == "comercial"

    def test_admin_e_material(self):
        assert _classificar("3.2.03.05") == "administrativo"
        assert _classificar("3.2.07.01") == "administrativo"

    def test_financeiro_tributos_excep(self):
        assert _classificar("3.2.04.12") == "financeiro"
        assert _classificar("3.2.05.01") == "tributos"
        assert _classificar("3.2.09.01") == "excepcional"
        assert _classificar("3.3.01") == "excepcional"

    def test_cmv_e_ativo_fora_dre(self):
        assert not _entra_dre("3.1.01.08")
        assert not _entra_dre("1.1.03.02")
        assert _classificar("1.1.03.02") == "outros"
