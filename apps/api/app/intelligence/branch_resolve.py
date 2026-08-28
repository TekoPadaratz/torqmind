"""Resolução de filial por apelido operacional (VR 01, posto VR, etc.)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from app.db import get_conn
from app.filial_apelido import load_apelido_map
from app.intelligence.normalize import fold_key, normalize_text

_COMPACT_RE = re.compile(r"[^a-z0-9]+")
_VR_NUM_RE = re.compile(r"(?:vr|rede)\s*[-]?\s*(\d{1,4})", re.I)
_WORD_NUM_RE = re.compile(r"([a-zà-ú]{2,12})\s*(\d{1,4})", re.I)


@dataclass
class BranchResolveResult:
    status: str  # skip | resolved | ambiguous | not_found | forbidden
    id_filial: Optional[int] = None
    label: Optional[str] = None
    candidates: list[dict[str, Any]] = field(default_factory=list)
    message: Optional[str] = None


def _compact(value: str | None) -> str:
    return _COMPACT_RE.sub("", fold_key(value or ""))


def _allowed_branch_ids(scope: dict[str, Any], claims: dict[str, Any]) -> list[int]:
    raw = scope.get("id_filial")
    if isinstance(raw, list):
        ids = [int(x) for x in raw if int(x) > 0]
        if ids:
            return ids
    if isinstance(raw, int) and raw > 0:
        return [int(raw)]
    filiais = scope.get("id_filiais")
    if isinstance(filiais, list):
        ids = [int(x) for x in filiais if int(x) > 0]
        if ids:
            return ids
    accesses = claims.get("accesses") or []
    out: set[int] = set()
    empresa = int(scope.get("id_empresa") or claims.get("id_empresa") or 0)
    for acc in accesses:
        if int(acc.get("id_empresa") or 0) != empresa:
            continue
        fid = acc.get("id_filial")
        if fid is None:
            continue
        try:
            out.add(int(fid))
        except (TypeError, ValueError):
            continue
    return sorted(out)


def _branch_catalog(id_empresa: int, allowed_ids: list[int]) -> dict[int, str]:
    """id_filial → label operacional (apelido preferido)."""
    if not allowed_ids:
        return {}
    labels: dict[int, str] = {}
    apelidos = load_apelido_map(id_empresa)
    for fid in allowed_ids:
        if apelidos.get(fid):
            labels[fid] = str(apelidos[fid]).strip()
    missing = [fid for fid in allowed_ids if fid not in labels]
    if missing:
        try:
            with get_conn(tenant_id=id_empresa) as conn:
                rows = conn.execute(
                    """
                    SELECT id_filial,
                           COALESCE(
                             NULLIF(TRIM(apelido), ''),
                             NULLIF(TRIM(nome), ''),
                             'Filial ' || id_filial::text
                           ) AS label
                    FROM auth.filiais
                    WHERE id_empresa = %s AND id_filial = ANY(%s)
                    """,
                    (id_empresa, missing),
                ).fetchall()
                for row in rows:
                    labels[int(row["id_filial"])] = str(row["label"]).strip()
        except Exception:
            pass
    for fid in allowed_ids:
        labels.setdefault(fid, f"Filial {fid}")
    return labels


def _extract_tokens(label: str) -> tuple[Optional[str], Optional[int]]:
    m = _VR_NUM_RE.search(label)
    if m:
        return "vr", int(m.group(1))
    m = _WORD_NUM_RE.search(label)
    if m:
        return m.group(1).casefold(), int(m.group(2))
    return None, None


def _score_hint(hint: str, label: str) -> float:
    h = _compact(hint)
    l = _compact(label)
    if not h or not l:
        return 0.0
    if h == l:
        return 1.0
    if h in l or l in h:
        return 0.92
    word_h, num_h = _extract_tokens(hint)
    word_l, num_l = _extract_tokens(label)
    if num_h is not None and num_l is not None and num_h == num_l:
        if word_h and word_l and word_h == word_l:
            return 0.95
        if word_h in {"vr", "rede"} and word_l in {"vr", "rede"}:
            return 0.95
        if not word_h or not word_l:
            return 0.88
    return 0.0


def resolve_branch_hint(
    hint: str | None,
    scope: dict[str, Any],
    claims: dict[str, Any],
) -> BranchResolveResult:
    """Resolve apelido mencionado na frase para id_filial no escopo do usuário."""
    cleaned = str(hint or "").strip()
    if not cleaned:
        return BranchResolveResult(status="skip")

    id_empresa = int(scope.get("id_empresa") or claims.get("id_empresa") or 0)
    if id_empresa <= 0:
        return BranchResolveResult(status="skip")

    allowed = _allowed_branch_ids(scope, claims)
    if not allowed:
        return BranchResolveResult(
            status="not_found",
            message="Não encontrei filiais no seu escopo para essa consulta.",
        )

    catalog = _branch_catalog(id_empresa, allowed)
    scored: list[tuple[float, int, str]] = []
    for fid, label in catalog.items():
        score = _score_hint(cleaned, label)
        if score > 0:
            scored.append((score, fid, label))

    if not scored:
        return BranchResolveResult(
            status="not_found",
            message=f"Não reconheci a filial “{cleaned}” no seu escopo. Tente o apelido curto (ex.: VR 01).",
        )

    scored.sort(key=lambda x: (-x[0], x[2].casefold()))
    top_score = scored[0][0]
    top = [s for s in scored if s[0] >= top_score - 0.02]

    if len(top) == 1:
        _, fid, label = top[0]
        return BranchResolveResult(status="resolved", id_filial=fid, label=label)

    options = [
        {"label": label, "value": fid, "id_filial": fid}
        for _, fid, label in top[:5]
    ]
    return BranchResolveResult(
        status="ambiguous",
        candidates=options,
        message=f"Encontrei mais de uma filial parecida com “{cleaned}”. Qual delas?",
    )


def apply_resolved_branch(scope: dict[str, Any], result: BranchResolveResult) -> dict[str, Any]:
    """Estreita o escopo para uma filial resolvida."""
    if result.status != "resolved" or result.id_filial is None:
        return scope
    out = dict(scope)
    out["id_filial"] = int(result.id_filial)
    out["id_filiais"] = [int(result.id_filial)]
    out["branch_scope"] = "selected"
    if result.label:
        out["filial_label"] = result.label
    return out
