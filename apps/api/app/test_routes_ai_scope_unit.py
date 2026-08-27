"""Regression: platform_master sem id_empresa não pode 500 no POST /ai/.../messages."""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from app.deps import get_current_claims
from app.main import app


PLATFORM_CLAIMS = {
    "sub": "00000000-0000-0000-0000-00000000a101",
    "user_role": "platform_master",
    "email": "ai-scope-test@torqmind.local",
    "id_empresa": None,
    "id_filial": None,
    "accesses": [],
    "access": {"product": True, "platform": True},
    "allowed_screens": ["assistant", "sales"],
    "can_view_sensitive_financials": True,
}


def test_ai_post_message_platform_master_without_claim_empresa_does_not_500():
    app.dependency_overrides[get_current_claims] = lambda: dict(PLATFORM_CLAIMS)
    client = TestClient(app)
    try:
        with patch("app.routes_ai.repos_ai.create_conversation") as create_conv, patch(
            "app.routes_ai.repos_ai.get_conversation"
        ) as get_conv, patch("app.routes_ai.repos_ai.add_message_pair") as add_pair, patch(
            "app.routes_ai.process_message"
        ) as process:
            create_conv.return_value = {
                "id": "11111111-1111-1111-1111-111111111111",
                "id_empresa": 1,
                "status": "active",
                "context_opaque": {},
                "message_count": 0,
            }
            get_conv.return_value = {
                "id": "11111111-1111-1111-1111-111111111111",
                "id_empresa": 1,
                "status": "active",
                "context_opaque": {},
                "message_count": 0,
            }
            process.return_value = {
                "status": "ok",
                "answer_text": "ok",
                "intent_id": "sales.overview",
                "tool_calls_meta": [],
            }
            add_pair.return_value = {
                "user_message_id": "22222222-2222-2222-2222-222222222222",
                "assistant_message": {"id": "33333333-3333-3333-3333-333333333333"},
            }

            created = client.post(
                "/ai/conversations",
                json={"title": "t", "id_empresa": 1, "id_filial": 14458},
            )
            assert created.status_code == 200, created.text
            cid = created.json()["id"]

            posted = client.post(
                f"/ai/conversations/{cid}/messages",
                json={
                    "text": "Na filial VR01, qual foi meu faturamento do mês até agora?",
                    "id_empresa": 1,
                    "id_filial": 14458,
                },
            )
            assert posted.status_code == 200, posted.text
            assert posted.json().get("status") == "ok"
            # get_conversation must receive scoped claims with id_empresa
            assert get_conv.called
            scoped_arg = get_conv.call_args.args[0]
            assert scoped_arg.get("id_empresa") == 1
    finally:
        app.dependency_overrides.pop(get_current_claims, None)
