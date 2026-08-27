"""Normalização tipográfica pt-BR para matching (preserva original para display)."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass


_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_WS_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9\s]+")


@dataclass(frozen=True)
class NormalizedText:
    original: str
    display: str
    lower: str
    fold: str  # sem acento, lower, tipografia tolerante


def _strip_accents(value: str) -> str:
    decomposed = unicodedata.normalize("NFD", value)
    return "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")


def _typo_fold(value: str) -> str:
    """Fold tipográfico leve (sem destruir números/documentos)."""
    text = value
    replacements = (
        ("ç", "c"),
        ("ñ", "n"),
        ("ß", "ss"),
    )
    for src, dst in replacements:
        text = text.replace(src, dst)
    # colapsa pontuação residual, mantém dígitos
    text = _NON_ALNUM_RE.sub(" ", text)
    text = _WS_RE.sub(" ", text).strip()
    return text


def normalize_text(raw: str | None) -> NormalizedText:
    original = str(raw or "")
    nfc = unicodedata.normalize("NFC", original)
    cleaned = _CONTROL_RE.sub("", nfc)
    # mantém newlines brevemente no display limpo, depois flatten para matching
    display = cleaned.strip()
    flattened = _WS_RE.sub(" ", display.replace("\r\n", "\n").replace("\r", "\n").replace("\n", " ")).strip()
    lower = flattened.casefold()
    fold = _typo_fold(_strip_accents(lower))
    return NormalizedText(original=original, display=display, lower=lower, fold=fold)


def fold_key(raw: str | None) -> str:
    return normalize_text(raw).fold
