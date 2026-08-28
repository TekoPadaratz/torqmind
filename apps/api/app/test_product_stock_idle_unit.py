"""Unit tests — Gestão de Produtos (estoque parado)."""
from __future__ import annotations

from unittest.mock import patch

from app.permissions import SCREEN_REGISTRY
from app.repos_product_management import (
    _setor_label,
    list_product_purchases_recent,
    list_product_stock_idle,
)
from app.scope import materialize_branch_query_targets


def test_product_management_screen_registered():
    meta = SCREEN_REGISTRY.get("product_management") or {}
    assert meta.get("label") == "Gestão de Produtos"
    assert meta.get("category") == "Operação"
    assert meta.get("has_sensitive") is True


def test_setor_label_mapping():
    assert _setor_label("conveniencia") == "Conveniência"
    assert _setor_label("automotivo") == "Automotivo"


def test_materialize_branch_query_targets_multi():
    filial, filiais = materialize_branch_query_targets([14458, 14459], None)
    assert filial is None
    assert filiais == [14458, 14459]


def test_materialize_branch_query_targets_single():
    filial, filiais = materialize_branch_query_targets(14458, None)
    assert filial == 14458
    assert filiais is None


@patch("app.repos_product_management.query_dict")
def test_list_product_stock_idle_maps_rows(mock_qd):
    mock_qd.side_effect = [
        [{"total": 1}],
        [
            {
                "id_filial": 14458,
                "filial_label": "VR 01",
                "id_produto": 42,
                "nome_produto": "ARLA 32",
                "setor_gerencial": "automotivo",
                "qtd_estoque": 10,
                "last_sale_date": "2026-08-01",
                "dias_sem_venda": 27,
                "custo_medio_compra": 2.5,
                "preco_venda": 3.29,
            }
        ],
        [{"setor_gerencial": "automotivo"}],
    ]
    out = list_product_stock_idle(1, id_filial=14458, min_dias_sem_venda=7)
    assert out["total"] == 1
    assert len(out["produtos"]) == 1
    p = out["produtos"][0]
    assert p["nome_produto"] == "ARLA 32"
    assert p["filial_label"] == "VR 01"
    assert p["custo_medio_total"] == 25.0
    assert p["receita_total"] == 32.9
    assert p["setor_label"] == "Automotivo"


@patch("app.repos_product_management.query_dict")
def test_list_product_purchases_recent(mock_qd):
    mock_qd.return_value = [
        {
            "rank": 1,
            "numero_documento": "8756",
            "data_compra": "2026-07-15",
            "qtd": 100,
            "valor_unitario": 2.4,
            "valor_total": 240.0,
        }
    ]
    out = list_product_purchases_recent(1, 14458, 42)
    assert out["id_produto"] == 42
    assert len(out["compras"]) == 1
    assert out["compras"][0]["numero_documento"] == "8756"
