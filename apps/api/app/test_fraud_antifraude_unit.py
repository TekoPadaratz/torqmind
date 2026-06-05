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
        "id_turno": 39430,
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
# _build_antifraude_event
# --------------------------------------------------------------------------- #
def test_build_event_rich_row_is_fully_resolved():
    ev = _build_antifraude_event(_rich_row())

    # date is real (no 1970, no "quando -")
    assert ev["data"] == "2026-06-03T09:00:00"
    assert ev["data_key"] == 20260603

    # real shift, not a user id
    assert ev["id_turno"] == 39430
    assert ev["turno_label"] == "Turno 39430"

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

    # modeled events are not tied to a single comprovante id
    assert ev["id_comprovante"] is None
    assert ev["event_id"] == "123456789"


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
    }
    ev = _build_antifraude_event(poor)
    assert ev["operador_label"] == "Operador sem cadastro"
    assert ev["turno_label"] == "Turno sem cadastro"
    assert ev["frentista_label"] == "Sem frentista associado"
    assert ev["filial_label"] == "Filial sem cadastro"
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
    # id_turno=1 is the upstream default that conflates many days/openings; it
    # must not be rendered as a real "Turno 1".
    row = _rich_row()
    row["id_turno"] = 1
    ev = _build_antifraude_event(row)
    assert ev["turno_label"] == "Turno sem cadastro"
    # documento_label must not fabricate "Turno 1"
    assert "Turno 1" not in ev["documento_label"]


# --------------------------------------------------------------------------- #
# risk_by_turn_local — the core regression
# --------------------------------------------------------------------------- #
def test_risk_by_turn_groups_by_real_shift_not_user():
    ch_row = {
        "id_filial": 14458,
        "filial_nome": "AUTO POSTO VR 01",
        "id_turno": 39430,
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
    # The semantic bug must never come back.
    assert "id_usuario AS id_turno" not in sql
    assert "id_turno > 1" in sql
    assert "GROUP BY id_filial, id_turno" in sql
    assert "mart_antifraude_eventos" in sql

    assert len(out) == 1
    row = out[0]
    assert row["id_turno"] == 39430
    assert row["turno_label"] == "Turno 39430"
    assert row["operador_label"] == "RAFAEL"  # replaces the fake "canal"
    assert row["eventos"] == 12
    assert row["alto_risco"] == 9
    assert row["impacto_estimado"] == 3200.0
    # No "canal" key is fabricated.
    assert "canal" not in row


# --------------------------------------------------------------------------- #
# risk_last_events — enriched feed
# --------------------------------------------------------------------------- #
def test_risk_last_events_reads_enriched_mart_and_is_resolved():
    with patch.object(repos_mart_realtime, "query_dict", return_value=[_rich_row()]) as qd:
        out = repos_mart_realtime.risk_last_events(
            "platform_master", 1, None, date(2026, 6, 1), date(2026, 6, 4), limit=30
        )

    sql = qd.call_args[0][0]
    assert "mart_antifraude_eventos" in sql
    assert "risk_recent_events_rt" not in sql

    assert len(out) == 1
    ev = out[0]
    assert ev["data"] == "2026-06-03T09:00:00"
    assert ev["operador_label"] == "TAYNA"
    assert ev["turno_label"] == "Turno 39430"
    assert ev["filial_label"] == "AUTO POSTO VR 01"
    assert ev["categoria"] == "Cancelamento da venda"
