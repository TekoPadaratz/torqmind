"""Evidence store in-memory por request — números citados precisam bater na evidência."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from typing import Any


_NUMBER_RE = re.compile(r"(?<![\w.])-?\d{1,3}(?:\.\d{3})*(?:,\d+)?|-?\d+(?:[.,]\d+)?")
_MONEY_ANSWER_RE = re.compile(r"R\$\s*(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)")
_COUNT_ANSWER_RE = re.compile(
    r"(?:com\s+(\d{1,3}(?:\.\d{3})*)\s+vendas|\((\d+)\s+título)"
)


def _canon_number(token: str) -> str:
    t = token.strip()
    if "," in t and "." in t:
        # 1.234,56
        t = t.replace(".", "").replace(",", ".")
    elif "," in t:
        t = t.replace(",", ".")
    return t


class EvidenceStore:
    def __init__(self) -> None:
        self._items: dict[str, dict[str, Any]] = {}

    def register(self, payload: Any, *, source: str | None = None) -> str:
        evidence_id = str(uuid.uuid4())
        blob = {
            "payload": payload,
            "source": source,
            "numbers_hash": self.hash_numbers(payload),
        }
        self._items[evidence_id] = blob
        return evidence_id

    def get(self, evidence_id: str) -> dict[str, Any] | None:
        return self._items.get(evidence_id)

    def ids(self) -> list[str]:
        return list(self._items.keys())

    @staticmethod
    def hash_numbers(payload: Any) -> str:
        nums = EvidenceStore.extract_numbers(payload)
        raw = "|".join(nums)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    @staticmethod
    def extract_numbers(payload: Any) -> list[str]:
        found: list[str] = []

        def walk(obj: Any) -> None:
            if obj is None:
                return
            if isinstance(obj, bool):
                return
            if isinstance(obj, (int, float)):
                found.append(_canon_number(str(obj)))
                return
            if isinstance(obj, str):
                for m in _NUMBER_RE.finditer(obj):
                    found.append(_canon_number(m.group(0)))
                return
            if isinstance(obj, dict):
                for v in obj.values():
                    walk(v)
                return
            if isinstance(obj, (list, tuple)):
                for v in obj:
                    walk(v)

        walk(payload)
        return found

    @staticmethod
    def extract_answer_numbers(answer: str) -> list[str]:
        """Números citados na resposta — evita falso positivo (ex.: 1437 → 143 + 7)."""
        text = answer or ""
        tokens: list[str] = []
        for m in _MONEY_ANSWER_RE.finditer(text):
            tokens.append(_canon_number(m.group(1)))
        for m in _COUNT_ANSWER_RE.finditer(text):
            raw = m.group(1) or m.group(2)
            if raw:
                tokens.append(raw.replace(".", ""))
        stripped = _MONEY_ANSWER_RE.sub("", text)
        stripped = _COUNT_ANSWER_RE.sub("", stripped)
        for m in re.finditer(r"\b(\d{3,})\b", stripped):
            tokens.append(m.group(1))
        return tokens

    def validate_numbers(self, answer: str, evidence_ids: list[str] | None = None) -> bool:
        """True se todo número 'forte' da resposta aparece em alguma evidência.

        Números curtos (1–2 dígitos) e anos (20xx) são tolerados.
        """
        ids = evidence_ids or list(self._items.keys())
        pool: set[str] = set()
        for eid in ids:
            item = self._items.get(eid)
            if not item:
                continue
            pool.update(self.extract_numbers(item.get("payload")))
        float_pool: list[float] = []
        for p in pool:
            try:
                float_pool.append(float(p))
            except ValueError:
                pass

        tokens = self.extract_answer_numbers(answer)
        if not tokens:
            for m in _NUMBER_RE.finditer(answer or ""):
                token = m.group(0)
                digits = re.sub(r"\D", "", token)
                if len(digits) <= 2:
                    continue
                if re.fullmatch(r"20\d{2}", digits):
                    continue
                tokens.append(_canon_number(token))

        for token in tokens:
            digits = re.sub(r"\D", "", token)
            if len(digits) <= 2:
                continue
            if re.fullmatch(r"20\d{2}", digits):
                continue
            canon = _canon_number(token)
            if canon in pool or digits in {re.sub(r"\D", "", p) for p in pool}:
                continue
            try:
                fv = float(canon)
                if any(abs(fv - pv) < 0.02 for pv in float_pool):
                    continue
            except ValueError:
                pass
            if not pool:
                return True
            return False
        return True

    def summary(self) -> list[dict[str, Any]]:
        out = []
        for eid, item in self._items.items():
            out.append(
                {
                    "evidence_id": eid,
                    "source": item.get("source"),
                    "numbers_hash": item.get("numbers_hash"),
                }
            )
        return out

    def dump_minimized(self) -> str:
        return json.dumps(self.summary(), ensure_ascii=False, sort_keys=True)
