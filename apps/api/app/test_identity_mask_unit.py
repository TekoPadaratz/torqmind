"""Unit tests for Hom demo identity mask (presentation layer)."""

from __future__ import annotations

import pytest

from app import identity_mask as im
from app.permissions import redact_sensitive
from app.runtime_guard import assert_runtime_stack_or_exit


@pytest.fixture(autouse=True)
def _reset_maps(monkeypatch):
    im.reset_identity_maps_for_tests()
    monkeypatch.setattr(
        "app.config.settings.demo_identity_mask",
        True,
        raising=False,
    )
    yield
    im.reset_identity_maps_for_tests()


def test_mask_disabled_passthrough(monkeypatch):
    monkeypatch.setattr("app.config.settings.demo_identity_mask", False, raising=False)
    payload = {"filial_label": "VR06", "total_venda": 100.0}
    assert im.mask_identity_payload(payload)["filial_label"] == "VR06"


def test_filial_and_person_stable_labels():
    payload = {
        "items": [
            {
                "id_filial": 11621,
                "filial_label": "VR06 - Verenka",
                "id_funcionario": 1038,
                "nome_vendedor": "Daniel Silva",
                "total_venda": 3673.39,
                "documento": "12345678901234567890123456789012345678901234",
            },
            {
                "id_filial": 11621,
                "filial_label": "VR06 - Verenka",
                "id_funcionario": 1038,
                "nome_vendedor": "Daniel Silva",
                "total_venda": 50.0,
            },
            {
                "id_filial": 99,
                "filial_nome": "Outra Filial",
                "nome_vendedor": "Outro",
                "id_funcionario": 7,
            },
        ]
    }
    out = im.mask_identity_payload(payload)
    row0, row1, row2 = out["items"]
    assert row0["filial_label"] == "Filial 1"
    assert row1["filial_label"] == "Filial 1"  # same id → same label
    assert row2["filial_nome"] == "Filial 2"
    assert row0["nome_vendedor"] == "Colaborador 1038"
    assert row1["nome_vendedor"] == "Colaborador 1038"
    assert row0["total_venda"] == 3673.39
    assert row0["documento"].startswith("12345")  # NF untouched


def test_cliente_cpf_email_masked():
    payload = {
        "cliente_nome": "Rede Verenka Ltda",
        "id_cliente": 12345,
        "cpf": "123.456.789-00",
        "cnpj": "12.345.678/0001-99",
        "email": "dono@verenka.com.br",
        "telefone": "11999998888",
        "documento": "35250123456789000123550010000012341123456789",
    }
    out = im.mask_identity_payload(payload)
    assert out["cliente_nome"] == "Cliente 12345"
    assert out["cpf"] == im._MASKED_CPF
    assert out["cnpj"] == im._MASKED_CNPJ
    assert out["email"] == im._MASKED_EMAIL
    assert out["telefone"] == im._MASKED_PHONE
    assert "Verenka" not in str(out)
    assert out["documento"].startswith("35250")


def test_list_filiais_shape():
    items = {
        "items": [
            {
                "id_filial": 11621,
                "nome": "VR06",
                "nome_completo": "Posto Verenka 06",
                "apelido": "VR06",
            }
        ]
    }
    out = im.mask_identity_payload(items)
    row = out["items"][0]
    assert row["nome"] == "Filial 1"
    assert row["apelido"] == "Filial 1"
    assert row["nome_completo"] == "Filial 1"
    assert "Verenka" not in str(out)


def test_branding_forced_default():
    public = im.mask_branding_public(
        {
            "id_empresa": 1,
            "background_url": "/api/branding/1/background?v=9",
            "logo_url": "/api/branding/1/logo?v=9",
            "background_version": 9,
            "logo_version": 9,
            "uses_default": False,
        }
    )
    assert public["uses_default"] is True
    assert public["logo_url"] is None
    assert public["background_url"] is None
    assert public["display_name"] == "Empresa 1"


def test_redact_sensitive_applies_identity_mask_even_for_owner():
    claims = {
        "user_role": "tenant_admin",
        "can_view_sensitive_financials": True,
    }
    payload = {
        "filial_label": "VR06",
        "id_filial": 10,
        "margem": 12.5,
        "nome_vendedor": "Ana",
        "id_funcionario": 5,
    }
    out = redact_sensitive(payload, claims)
    assert out["margem"] == 12.5  # financial kept for owner
    assert out["filial_label"] == "Filial 1"
    assert out["nome_vendedor"] == "Colaborador 5"


def test_cache_version_toggles(monkeypatch):
    assert im.identity_mask_cache_version() == im.IDENTITY_MASK_VERSION
    monkeypatch.setattr("app.config.settings.demo_identity_mask", False, raising=False)
    assert im.identity_mask_cache_version() == 0


def test_prod_stack_rejects_demo_identity_mask(monkeypatch):
    monkeypatch.setenv("TORQMIND_STACK", "prod")
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/torqmind")
    monkeypatch.setenv("PG_DATABASE", "torqmind")
    monkeypatch.setenv("DEMO_IDENTITY_MASK", "true")
    with pytest.raises(SystemExit) as exc:
        assert_runtime_stack_or_exit()
    assert exc.value.code == 2


def test_prod_stack_accepts_mask_off(monkeypatch):
    monkeypatch.setenv("TORQMIND_STACK", "prod")
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/torqmind")
    monkeypatch.setenv("PG_DATABASE", "torqmind")
    monkeypatch.setenv("DEMO_IDENTITY_MASK", "false")
    assert_runtime_stack_or_exit()


def test_bank_and_filial_nick_free_text():
    payload = {
        "filiais": [{"id_filial": 11621, "nome": "VR 07", "filial_label": "VR 07"}],
        "secoes": [
            {
                "chave": "banco",
                "itens": [
                    {"label": "CONTA VR07 AG:9247 C/C:41400-7", "valor": 100.0},
                    {"descricao": "CONTA VR09 AG: 0626 C/C: 50516-8", "valor": 1.0},
                    {"label": "Combustível", "valor": 50.0},
                ],
            }
        ],
        "vendedores": [
            {"id_funcionario": 10, "nome_vendedor": "JURIT DIAS QUINTAS", "comissao_estimada": 99.0}
        ],
    }
    out = im.mask_identity_payload(payload)
    assert out["filiais"][0]["filial_label"] == "Filial 1"
    assert out["filiais"][0]["nome"] == "Filial 1"
    assert out["secoes"][0]["itens"][0]["label"].startswith("Conta ")
    assert "VR07" not in out["secoes"][0]["itens"][0]["label"]
    assert out["secoes"][0]["itens"][2]["label"] == "Combustível"
    assert out["vendedores"][0]["nome_vendedor"] == "Colaborador 10"
    assert out["vendedores"][0]["comissao_estimada"] == 99.0


def test_usuario_nome_and_star_nome_catchall():
    out = im.mask_identity_payload(
        {
            "top_users": [
                {"usuario_nome": "LUCIANA", "id_usuario": 55, "id_filial": 1},
                {"usuario_nome": "ANGELUZA", "id_filial": 2},
            ],
            "turnos": [{"usuario_nome": "JORGE D", "turno_label": "Turno 2"}],
            "last_events": [
                {
                    "operador_caixa_label": "PRICILA M",
                    "responsavel_label": "PRICILA M",
                    "frentista_label": "Sem frentista associado",
                    "funcionario_label": "JULIANA",
                }
            ],
        }
    )
    assert out["top_users"][0]["usuario_nome"] == "Colaborador 55"
    assert out["top_users"][1]["usuario_nome"].startswith("Colaborador")
    assert out["turnos"][0]["usuario_nome"].startswith("Colaborador")
    assert out["turnos"][0]["turno_label"] == "Turno 2"
    assert out["last_events"][0]["operador_caixa_label"].startswith("Colaborador")
    assert out["last_events"][0]["responsavel_label"].startswith("Colaborador")
    assert out["last_events"][0]["frentista_label"] == "Sem frentista associado"
    assert out["last_events"][0]["funcionario_label"].startswith("Colaborador")


def test_empresa_selector_names():
    session_bit = {
        "id_empresa": 1,
        "product_companies": [
            {"id_empresa": 1, "tenant_name": "Rede Verenka"},
            {"id_empresa": 2, "tenant_name": "Outra Rede"},
        ],
        "accesses": [
            {
                "id_empresa": 1,
                "tenant_name": "Rede Verenka",
                "id_filial": 11621,
                "branch_name": "VR06",
            }
        ],
        "email": "admin@verenka.com",
        "name": "Teko Admin",
    }
    out = im.mask_identity_payload(session_bit)
    assert out["product_companies"][0]["tenant_name"].startswith("Empresa")
    assert "Verenka" not in str(out)
    assert out["email"] == im._MASKED_EMAIL
    assert out["name"].startswith("Colaborador")
    assert out["accesses"][0]["branch_name"] == "Filial 1"
