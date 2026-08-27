"""Limites operacionais do Assistente TorqMind (somente leitura)."""

from __future__ import annotations

from dataclasses import dataclass


MAX_MESSAGE_CHARS = 2000
MAX_RESPONSE_CHARS = 8000
MAX_MESSAGES_PER_CONVERSATION = 100
MAX_ACTIVE_CONVERSATIONS = 50
MAX_CLARIFICATION_OPTIONS = 5
MAX_SUGGESTIONS = 6
MAX_TOOL_ROWS = 50
MAX_EVIDENCE_ITEMS = 20
DEFAULT_TOOL_TIMEOUT_S = 12
RATE_LIMIT_PER_MINUTE = 30

CONFIDENCE_EXECUTE = 0.92
CONFIDENCE_CLARIFY = 0.70


@dataclass(frozen=True)
class IntelligenceLimits:
    max_message_chars: int = MAX_MESSAGE_CHARS
    max_response_chars: int = MAX_RESPONSE_CHARS
    max_messages_per_conversation: int = MAX_MESSAGES_PER_CONVERSATION
    max_active_conversations: int = MAX_ACTIVE_CONVERSATIONS
    max_clarification_options: int = MAX_CLARIFICATION_OPTIONS
    max_suggestions: int = MAX_SUGGESTIONS
    max_tool_rows: int = MAX_TOOL_ROWS
    max_evidence_items: int = MAX_EVIDENCE_ITEMS
    default_tool_timeout_s: int = DEFAULT_TOOL_TIMEOUT_S
    rate_limit_per_minute: int = RATE_LIMIT_PER_MINUTE
    confidence_execute: float = CONFIDENCE_EXECUTE
    confidence_clarify: float = CONFIDENCE_CLARIFY


def get_limits() -> IntelligenceLimits:
    """Bridge opcional com Settings quando os campos existirem."""
    try:
        from app.config import settings

        return IntelligenceLimits(
            max_message_chars=int(getattr(settings, "ai_chat_max_message_chars", MAX_MESSAGE_CHARS) or MAX_MESSAGE_CHARS),
            rate_limit_per_minute=int(
                getattr(settings, "ai_chat_rate_limit_per_minute", RATE_LIMIT_PER_MINUTE) or RATE_LIMIT_PER_MINUTE
            ),
        )
    except Exception:
        return IntelligenceLimits()
