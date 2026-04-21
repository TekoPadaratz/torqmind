from __future__ import annotations

import re

USERNAME_PATTERN = re.compile(r"^[a-z0-9._-]{3,32}$")
USERNAME_RULES_MESSAGE = (
    "Nome de usuário deve ter entre 3 e 32 caracteres e usar apenas letras minúsculas, números, ponto, underscore ou hífen."
)


def normalize_email(value: str) -> str:
    return str(value or "").strip().lower()


def normalize_login_identifier(value: str) -> str:
    return str(value or "").strip()


def normalize_username(value: str) -> str:
    return normalize_login_identifier(value).lower()


def username_from_email_candidate(email: str) -> str:
    normalized_email = normalize_email(email)
    local_part, _, _ = normalized_email.partition("@")
    return local_part or normalized_email


def fallback_username_for_user_id(user_id: str) -> str:
    compact = str(user_id or "").replace("-", "").lower()
    return f"u-{compact[:30]}"


def is_valid_username(value: str) -> bool:
    return bool(USERNAME_PATTERN.fullmatch(normalize_username(value)))


def validate_username(value: str) -> str:
    normalized = normalize_username(value)
    if not USERNAME_PATTERN.fullmatch(normalized):
        raise ValueError(USERNAME_RULES_MESSAGE)
    return normalized


def identifier_looks_like_email(value: str) -> bool:
    return "@" in normalize_login_identifier(value)
