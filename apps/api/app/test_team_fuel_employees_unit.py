"""Unit tests — ranking combustível por funcionário (Equipe)."""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

from app.repos_mart_realtime import team_fuel_employees_dashboard


@patch("app.filial_apelido.apelido_for", return_value="VR 01")
@patch("app.repos_mart_realtime.query_scalar", return_value=0)
@patch("app.repos_mart_realtime.query_dict")
def test_team_fuel_employees_dashboard_groups_by_filial(mock_qd, _mock_scalar, _mock_apelido):
    mock_qd.side_effect = [
        # qty_abastecimentos (slim count)
        [
            {"id_filial": 10169, "id_funcionario": 10, "qtd_abastecimentos": 15},
            {"id_filial": 10169, "id_funcionario": 20, "qtd_abastecimentos": 8},
        ],
        # litros rows (fallback slim path — mart_count=0)
        [
            {
                "id_filial": 10169,
                "id_funcionario": 10,
                "id_produto": 1,
                "nome_produto": "GASOLINA C COMUM",
                "nome_grupo": "COMBUSTIVEIS",
                "litros": 1200.0,
            },
            {
                "id_filial": 10169,
                "id_funcionario": 10,
                "id_produto": 2,
                "nome_produto": "ETANOL COMUM",
                "nome_grupo": "COMBUSTIVEIS",
                "litros": 300.0,
            },
            {
                "id_filial": 10169,
                "id_funcionario": 20,
                "id_produto": 1,
                "nome_produto": "GASOLINA C COMUM",
                "nome_grupo": "COMBUSTIVEIS",
                "litros": 800.0,
            },
        ],
        [{"id_filial": 10169, "id_funcionario": 10, "nome": "JOAO"}, {"id_filial": 10169, "id_funcionario": 20, "nome": "MARIA"}],
    ]
    out = team_fuel_employees_dashboard(
        "platform_master",
        1,
        10169,
        date(2026, 7, 1),
        date(2026, 7, 31),
    )
    assert len(out["filiais"]) == 1
    filial = out["filiais"][0]
    assert filial["filial_label"] == "VR 01"
    assert filial["total_litros"] == 2300.0
    assert filial["total_abastecimentos"] == 23
    assert filial["ranking"][0]["id_funcionario"] == 10
    assert filial["ranking"][0]["litros"] == 1500.0
    assert filial["ranking"][0]["qtd_abastecimentos"] == 15
    assert len(filial["combustiveis"]) == 2
    assert filial["by_employee"]["10"]["total_litros"] == 1500.0
    assert filial["by_employee"]["10"]["qtd_abastecimentos"] == 15