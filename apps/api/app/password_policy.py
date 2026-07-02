"""TorqMind password policy (canonical).

PT-BR: Política única de composição de senha, usada no cadastro/alteração e na
recuperação de senha. O frontend espelha estas regras em
``apps/web/app/lib/password-policy.mjs`` — mantenha os dois em sincronia.

EN: Single source of truth for password composition rules, shared by user
creation/change and password reset. The frontend mirrors these rules in
``apps/web/app/lib/password-policy.mjs`` — keep both in sync.
"""

from __future__ import annotations

import re
from typing import Callable, List

PASSWORD_MIN_LENGTH = 8
PASSWORD_MAX_LENGTH = 128

# Cada regra: (chave estável, descrição pt-br para o usuário, verificação).
_RULES: List[tuple[str, str, Callable[[str], bool]]] = [
    ("length", f"Pelo menos {PASSWORD_MIN_LENGTH} caracteres", lambda p: len(p) >= PASSWORD_MIN_LENGTH),
    ("lowercase", "Uma letra minúscula (a-z)", lambda p: bool(re.search(r"[a-z]", p))),
    ("uppercase", "Uma letra maiúscula (A-Z)", lambda p: bool(re.search(r"[A-Z]", p))),
    ("digit", "Um número (0-9)", lambda p: bool(re.search(r"\d", p))),
    ("special", "Um caractere especial (ex.: ! @ # $ % & *)", lambda p: bool(re.search(r"[^A-Za-z0-9]", p))),
]


def validate_password(password: str) -> List[str]:
    """Return a list of unmet-rule descriptions. Empty list means the password is valid."""
    pw = password or ""
    errors = [desc for _key, desc, check in _RULES if not check(pw)]
    if len(pw) > PASSWORD_MAX_LENGTH:
        errors.append(f"No máximo {PASSWORD_MAX_LENGTH} caracteres")
    return errors


def is_valid_password(password: str) -> bool:
    return not validate_password(password)


def describe_rules() -> List[dict]:
    """Machine-readable rule list for surfacing the policy to clients."""
    return [{"key": key, "label": desc} for key, desc, _check in _RULES]


def policy_message() -> str:
    """Single-line human description of the policy (pt-br)."""
    return "A senha deve conter: " + "; ".join(desc for _k, desc, _c in _RULES) + "."
