from decimal import Decimal

from app.intelligence.conversation import update_after_turn
from app.intelligence.json_util import dumps_json, json_ready


def test_json_ready_decimal():
    assert json_ready(Decimal("1234.56")) == 1234.56


def test_dumps_json_conversation_context_with_decimal():
    ctx = update_after_turn(
        {},
        intent_id="customer.open_titles",
        slots={"customer_name": "Vinicius"},
        period=None,
        entities=[{"nome": "VINICIUS", "total_compras_30d": Decimal("10.50")}],
        pending={"kind": "customer", "options": [{"label": "VINICIUS"}]},
    )
    raw = dumps_json(ctx)
    assert "10.5" in raw
    assert "Vinicius" in raw
