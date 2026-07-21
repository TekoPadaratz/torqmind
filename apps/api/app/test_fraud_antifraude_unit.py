"""Unit tests for the antifraud screen reconciliation (2026-06-04).

Two semantic bugs were fixed in ``repos_mart_realtime``:

* ``risk_last_events`` used to read the poor ``risk_recent_events_rt`` table,
  which returned no date, no real shift and no operator name. The screen showed
  "Operador sem cadastro", "Turno sem cadastro" and "quando -". It now reads the
  enriched ``mart_antifraude_eventos`` and emits a complete contract.
* ``risk_by_turn_local`` grouped by ``id_usuario AS id_turno`` — a user is *not*
  a shift. The "Concentração por turno" block was therefore meaningless and a
  fake "canal não informado" column was rendered. It now groups by the real
  ``id_turno`` and surfaces the operator most associated with the shift.

These tests lock in the corrected behaviour and guard against regressions.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

from app import repos_mart_realtime
from app.repos_mart_realtime import (
    _antifraude_event_labels,
    _antifraude_turno_label,
    _antifraude_documento,
    _build_antifraude_event,
)


def _rich_row() -> dict:
    return {
        "event_id": 123456789,
        "id_filial": 14458,
        "filial_nome": "AUTO POSTO VR 01",
        "data_key": 20260603,
        "dt": "2026-06-03",
        "hora": 9,
        "event_type": "cancelamento",
        "source": "stg.comprovantes",
        "id_turno": 34292,          # technical ID_TURNOS — never shown as shift
        "turno_numero": 3,          # real operational shift
        "id_caixa": 0,
        "id_usuario": 51,
        "nome_operador": "TAYNA",
        "id_funcionario": 77,
        "nome_funcionario": "ALYNE",
        "valor_total": 250.5,
        "impacto_estimado": 250.5,
        "score_risco": 80,
        "score_level": "HIGH",
        "reasons": '{"source":"stg.comprovantes","rule":"cancelled_receipt"}',
        "id_comprovante": 3587794,  # technical PK (never shown as documento)
        "nro_comprovante": 503752,  # NROCOMPROVANTE — never shown as documento
        "numero_nfe": "325152",     # DOCUMENTO = NF-e/NFC-e
    }


# --------------------------------------------------------------------------- #
# _antifraude_event_labels
# --------------------------------------------------------------------------- #
def test_labels_cancelamento_cancelled_receipt():
    categoria, motivo = _antifraude_event_labels(
        "cancelamento", '{"source":"stg.comprovantes","rule":"cancelled_receipt"}'
    )
    assert categoria == "Cancelamento da venda"
    assert motivo == "Comprovante cancelado na origem (cancelamento de venda)."


def test_labels_unknown_event_has_safe_fallback():
    categoria, motivo = _antifraude_event_labels("algo_novo", "{}")
    assert categoria == "Alerta relevante"
    assert motivo  # never empty, never "sem cadastro"
    assert "sem cadastro" not in motivo.lower()


def test_labels_tolerates_broken_reasons_json():
    categoria, motivo = _antifraude_event_labels("cancelamento", "not-json")
    assert categoria == "Cancelamento da venda"
    assert motivo  # falls back without raising


# --------------------------------------------------------------------------- #
# _antifraude_turno_label  (FASE 2 — operational shift vs technical id)
# --------------------------------------------------------------------------- #
def test_turno_label_operational_number_not_technical_id():
    label, resolved = _antifraude_turno_label(3, 34292)
    assert label == "Turno 3"
    assert resolved is True
    assert "34292" not in label


def test_turno_label_zero_is_caixa_geral():
    label, resolved = _antifraude_turno_label(0, 21731)
    assert label == "Caixa geral"
    assert resolved is True


def test_turno_label_unresolved_when_no_shift():
    label, resolved = _antifraude_turno_label(0, 0)
    assert label == "Turno não resolvido"
    assert resolved is False
    assert "não identificad" not in label.lower()


# --------------------------------------------------------------------------- #
# _antifraude_documento  (DOCUMENTO = NF-e/NFC-e only)
# --------------------------------------------------------------------------- #
def test_documento_prefers_nota_fiscal():
    venda, label, source, fiscal = _antifraude_documento("987654", 503752, 3587794)
    assert venda == "987654"
    assert label == "987654"
    assert source == "nota_fiscal"
    assert fiscal == "987654"


def test_documento_without_nfe_is_dash_never_comprovante():
    venda, label, source, fiscal = _antifraude_documento("", 503752, 3587794)
    assert venda is None
    assert label == "—"
    assert source == "fallback"
    assert fiscal is None


def test_documento_never_uses_id_comprovante():
    venda, label, source, fiscal = _antifraude_documento("", 0, 3587794)
    assert venda is None
    assert label == "—"
    assert source == "fallback"
    assert fiscal is None


def test_documento_never_turno_or_filial():
    _, label, _, _ = _antifraude_documento("987654", 503752, 3587794)
    assert "Turno" not in label
    assert "·" not in label
    assert "Comprovante" not in label


# --------------------------------------------------------------------------- #
# _build_antifraude_event
# --------------------------------------------------------------------------- #
def test_build_event_rich_row_is_fully_resolved():
    ev = _build_antifraude_event(_rich_row())

    # date is real (no 1970, no "quando -")
    assert ev["data"] == "2026-06-03T09:00:00"
    assert ev["data_key"] == 20260603

    # operational shift, NOT the technical id
    assert ev["turno_numero"] == 3
    assert ev["turno_label"] == "Turno 3"
    assert ev["id_turno"] == 34292            # technical kept for traceability
    assert "34292" not in ev["turno_label"]   # but never rendered as the shift

    # documento = NF-e/NFC-e only (never comprovante / id técnico)
    assert ev["id_comprovante"] == 3587794
    assert ev["documento_venda"] == "325152"
    assert ev["documento_label"] == "325152"
    assert ev["documento_source"] == "nota_fiscal"
    assert ev["documento_fiscal"] == "325152"
    assert "Turno" not in ev["documento_label"]
    assert "AUTO POSTO" not in ev["documento_label"]
    assert "503752" not in ev["documento_label"]
    assert "3587794" not in ev["documento_label"]

    # operator resolved by name, mirrored across the label aliases the UI reads
    assert ev["operador_label"] == "TAYNA"
    assert ev["operador_caixa_label"] == "TAYNA"
    assert ev["responsavel_label"] == "TAYNA"
    assert ev["usuario_label"] == "TAYNA"
    assert ev["usuario_source"] == "comprovante"

    # branch + frentista resolved
    assert ev["filial_label"] == "AUTO POSTO VR 01"
    assert ev["frentista_label"] == "ALYNE"

    # category / reason from the rule
    assert ev["categoria"] == "Cancelamento da venda"
    assert ev["motivo"].startswith("Comprovante cancelado na origem")

    # score + impact preserved
    assert ev["score"] == 80
    assert ev["score_level"] == "HIGH"
    assert ev["impacto_estimado"] == 250.5
    assert ev["event_id"] == "123456789"


def test_build_event_without_nfe_shows_dash():
    row = _rich_row()
    row["numero_nfe"] = ""
    row["nro_comprovante"] = 0
    ev = _build_antifraude_event(row)
    assert ev["documento_label"] == "—"
    assert ev["documento_source"] == "fallback"
    assert ev["documento_fiscal"] is None


def test_build_event_poor_row_uses_house_style_fallbacks():
    poor = {
        "event_id": None,
        "id_filial": 0,
        "filial_nome": "",
        "data_key": 0,
        "dt": None,
        "hora": 0,
        "event_type": "cancelamento",
        "id_turno": 0,
        "turno_numero": 0,
        "id_caixa": 0,
        "id_usuario": 0,
        "nome_operador": "",
        "id_funcionario": None,
        "nome_funcionario": "",
        "valor_total": None,
        "impacto_estimado": 0,
        "score_risco": 0,
        "score_level": "",
        "reasons": "{}",
        "id_comprovante": 0,
        "nro_comprovante": 0,
    }
    ev = _build_antifraude_event(poor)
    assert ev["operador_label"] == "Operador sem cadastro"
    assert ev["turno_label"] == "Turno não resolvido"
    assert ev["frentista_label"] == "Sem frentista associado"
    assert ev["filial_label"] == "Filial sem cadastro"
    assert ev["documento_label"] == "—"
    # Never emit the lint-prohibited "não identificado" wording on any label.
    for key in ("operador_label", "turno_label", "frentista_label", "filial_label"):
        assert "não identificad" not in str(ev[key]).lower()


def test_build_event_id_only_operator_fallback():
    row = _rich_row()
    row["nome_operador"] = ""
    row["id_usuario"] = 51
    ev = _build_antifraude_event(row)
    assert ev["operador_label"] == "Operador #51"
    assert ev["usuario_source"] == "id_only"


def test_build_event_treats_turno_1_as_unresolved_sentinel():
    # turno_numero 0 + technical id_turno=1 (sentinel) => unresolved, never "Turno 1".
    row = _rich_row()
    row["id_turno"] = 1
    row["turno_numero"] = 0
    ev = _build_antifraude_event(row)
    assert ev["turno_label"] == "Turno não resolvido"
    assert "Turno 1" not in ev["documento_label"]


# --------------------------------------------------------------------------- #
# risk_by_turn_local — concentration by operational shift
# --------------------------------------------------------------------------- #
def test_risk_by_turn_groups_by_operational_shift_not_user_or_tech_id():
    ch_row = {
        "id_filial": 14458,
        "filial_nome": "AUTO POSTO VR 01",
        "turno_numero": 3,
        "eventos": 12,
        "alto_risco": 9,
        "impacto_estimado": 3200.0,
        "score_medio": 79.5,
        "operador_top": "RAFAEL",
    }
    with patch.object(repos_mart_realtime, "query_dict", return_value=[ch_row]) as qd:
        out = repos_mart_realtime.risk_by_turn_local(
            "platform_master", 1, None, date(2026, 6, 1), date(2026, 6, 4)
        )

    sql = qd.call_args[0][0]
    # neither semantic bug may come back
    assert "id_usuario AS id_turno" not in sql
    assert "turno_numero >= 1" in sql
    assert "GROUP BY id_filial, turno_numero" in sql
    assert "mart_antifraude_eventos" in sql

    assert len(out) == 1
    row = out[0]
    assert row["turno_numero"] == 3
    assert row["turno_label"] == "Turno 3"
    assert row["operador_label"] == "RAFAEL"   # replaces the fake "canal"
    assert row["eventos"] == 12
    # No "canal" key is fabricated.
    assert "canal" not in row


# --------------------------------------------------------------------------- #
# risk_last_events — enriched feed
# --------------------------------------------------------------------------- #
def test_risk_last_events_reads_enriched_mart_and_is_resolved():
    with patch.object(repos_mart_realtime, "query_dict", return_value=[_rich_row()]) as qd, patch.object(
        repos_mart_realtime,
        "_load_nfe_numbers",
        return_value={(14458, 3587794): "325152"},
    ):
        out = repos_mart_realtime.risk_last_events(
            "platform_master", 1, None, date(2026, 6, 1), date(2026, 6, 4), limit=30
        )

    sql = qd.call_args_list[0].args[0]
    assert "mart_antifraude_eventos" in sql
    assert "risk_recent_events_rt" not in sql
    assert "turno_numero" in sql
    assert "nro_comprovante" in sql

    assert len(out) == 1
    ev = out[0]
    assert ev["data"] == "2026-06-03T09:00:00"
    assert ev["operador_label"] == "TAYNA"
    assert ev["turno_label"] == "Turno 3"
    assert ev["documento_label"] == "325152"
    assert ev["documento_source"] == "nota_fiscal"
    assert ev["filial_label"] == "AUTO POSTO VR 01"
    assert ev["categoria"] == "Cancelamento da venda"
