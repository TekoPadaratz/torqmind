"""Deterministic corpus expander for TorqMind Intelligence evals.

Usage:
  python -m app.intelligence.evals.generate_corpus --write
  python -m app.intelligence.evals.generate_corpus --count
"""
from __future__ import annotations

import argparse
import hashlib
import json
import itertools
import re
import unicodedata
from pathlib import Path
from typing import Any, Iterable, Optional

_DATA_DIR = Path(__file__).resolve().parents[1] / "data"
_SEEDS_PATH = _DATA_DIR / "seed_questions_v1.json"
_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "capability_map" / "catalog_v1.json"
)

_PERIOD_VARIANTS = [
    "hoje",
    "ontem",
    "esta semana",
    "semana passada",
    "este mês",
    "mês passado",
    "últimos 7 dias",
    "últimos 30 dias",
    "agosto",
    "no período",
]

_SCOPE_VARIANTS = [
    "",
    "da filial 1",
    "da filial 2",
    "da rede",
    "do posto",
    "consolidado",
]

_TYPO_MAP = {
    "faturamento": ["faturamneto", "faturramento", "fat"],
    "vendas": ["venda", "vendass", "vndas"],
    "cliente": ["clente", "cliene", "client"],
    "inadimplência": ["inadimplencia", "inadimplensia", "inad"],
    "lucro": ["lucrro", "luc"],
    "meta": ["metaa", "mta"],
    "cancelamentos": ["cancelamntos", "cancelamento"],
    "estoque": ["estoqe", "estoq"],
    "comissão": ["comissao", "comssa"],
    "despesas": ["despeza", "despesass"],
}

_ACCENT_STRIP = str.maketrans(
    "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ",
    "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC",
)


def load_seeds(path: Optional[Path] = None) -> list[dict[str, Any]]:
    """Load seed_questions_v1.json."""
    p = path or _SEEDS_PATH
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("seed_questions_v1.json deve ser uma lista")
    return data


def _load_catalog_synonyms() -> dict[str, list[str]]:
    if not _CATALOG_PATH.exists():
        return {}
    cat = json.loads(_CATALOG_PATH.read_text(encoding="utf-8"))
    out: dict[str, list[str]] = {}
    for intent in cat.get("intents") or []:
        iid = intent.get("intent_id")
        if iid:
            out[iid] = list(intent.get("synonyms") or [])
    return out


def _strip_accents(text: str) -> str:
    return text.translate(_ACCENT_STRIP)


def _typo_variants(text: str) -> list[str]:
    out = []
    low = text.lower()
    for word, typos in _TYPO_MAP.items():
        if word in low:
            for typo in typos[:2]:
                out.append(re.sub(re.escape(word), typo, text, count=1, flags=re.I))
    return out


def _period_variants(text: str) -> list[str]:
    out = []
    # If text already has a period cue, still add one alternate
    for period in _PERIOD_VARIANTS[:6]:
        if period.lower() in text.lower():
            continue
        out.append(f"{text.rstrip(' ?')} {period}")
        if len(out) >= 3:
            break
    return out


def _scope_variants(text: str) -> list[str]:
    out = []
    for scope in _SCOPE_VARIANTS[1:4]:
        if scope and scope.lower() in text.lower():
            continue
        out.append(f"{text.rstrip(' ?')} {scope}".strip())
    return out


def _informal_variants(text: str) -> list[str]:
    return [
        text.lower(),
        text.replace("Qual ", "Q ").replace("Quanto ", "Qto "),
        text.replace("?", ""),
        f"{text} pf",
        f"{text} por favor",
    ]


def expand_seed(seed: dict[str, Any], synonyms: list[str]) -> list[str]:
    """Expand one seed into distinct formulation strings."""
    base = str(seed.get("text") or "").strip()
    if not base:
        return []
    forms = {base}
    forms.add(_strip_accents(base))
    for v in _typo_variants(base):
        forms.add(v)
    for v in _period_variants(base):
        forms.add(v)
        forms.add(_strip_accents(v))
    for v in _scope_variants(base):
        forms.add(v)
    for v in _informal_variants(base):
        forms.add(v.strip())
    # synonym swap for first contentful token-ish
    for syn in synonyms[:5]:
        forms.add(f"{syn} {base}".strip()[:200])
        forms.add(f"Me fala {syn}")
        forms.add(f"Quanto de {syn}?")
    # unicode normalization form
    forms.add(unicodedata.normalize("NFKC", base))
    return sorted(f for f in forms if f)


def count_formulations(seeds: Optional[list[dict[str, Any]]] = None) -> int:
    """Count distinct expanded formulations across seeds."""
    seeds = seeds if seeds is not None else load_seeds()
    syn_map = _load_catalog_synonyms()
    all_forms: set[str] = set()
    for seed in seeds:
        iid = str(seed.get("intent_id") or "")
        for f in expand_seed(seed, syn_map.get(iid, [])):
            all_forms.add(f.casefold().strip())
    return len(all_forms)


def generate_formulations(
    seeds: Optional[list[dict[str, Any]]] = None,
    *,
    min_count: int = 1500,
) -> list[dict[str, Any]]:
    """Expand seeds to at least min_count distinct formulations."""
    seeds = seeds if seeds is not None else load_seeds()
    syn_map = _load_catalog_synonyms()
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(text: str, seed: dict[str, Any], kind: str) -> None:
        key = text.casefold().strip()
        if not key or key in seen:
            return
        seen.add(key)
        rows.append(
            {
                "id": "form_" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:12],
                "text": text,
                "intent_id": seed.get("intent_id"),
                "seed_id": seed.get("id"),
                "domain": seed.get("domain"),
                "kind": kind,
                "expected_status": seed.get("expected_status"),
            }
        )

    for seed in seeds:
        iid = str(seed.get("intent_id") or "")
        syns = syn_map.get(iid, [])
        for f in expand_seed(seed, syns):
            add(f, seed, "expand")

    # Deterministic padding via combinatorial templates if short
    templates = [
        "{syn} {period}",
        "Me mostra {syn} {scope}",
        "{syn} {scope} {period}",
        "Qual {syn} {period}?",
        "qto {syn} {period}",
        "{syn} pf {period}",
    ]
    periods = _PERIOD_VARIANTS
    scopes = [s for s in _SCOPE_VARIANTS if s]
    intent_syns = [(iid, syns) for iid, syns in syn_map.items() if syns]
    pad_i = 0
    while len(rows) < min_count and intent_syns:
        iid, syns = intent_syns[pad_i % len(intent_syns)]
        syn = syns[pad_i % len(syns)]
        period = periods[pad_i % len(periods)]
        scope = scopes[pad_i % len(scopes)]
        tpl = templates[pad_i % len(templates)]
        text = tpl.format(syn=syn, period=period, scope=scope)
        seed = {
            "id": f"pad_{iid}",
            "intent_id": iid,
            "domain": iid.split(".", 1)[0],
        }
        add(text, seed, "combinatorial")
        pad_i += 1
        if pad_i > min_count * 20:
            break
    return rows


def generate_regression_cases(
    n: int = 5000,
    seeds: Optional[list[dict[str, Any]]] = None,
) -> list[dict[str, Any]]:
    """Combinatorial regression matrix (>= n cases).

    Matrix axes: intent × period × scope × role_flag × status_hint
    """
    seeds = seeds if seeds is not None else load_seeds()
    syn_map = _load_catalog_synonyms()
    intents = sorted(syn_map.keys()) or sorted(
        {str(s.get("intent_id")) for s in seeds if s.get("intent_id")}
    )
    periods = _PERIOD_VARIANTS
    scopes = _SCOPE_VARIANTS
    roles = ["owner", "tenant_manager", "tenant_kiosk", "tenant_viewer"]
    statuses = [None, "ok", "forbidden", "unsupported", "mutation_denied", "stale_data"]

    cases: list[dict[str, Any]] = []
    seen: set[str] = set()

    # Seed-grounded cases first
    for seed in seeds:
        for period, scope in itertools.product(periods[:4], scopes[:3]):
            text = f"{seed.get('text')} {period} {scope}".strip()
            key = hashlib.sha1(
                f"{seed.get('intent_id')}|{text}|{period}|{scope}".encode()
            ).hexdigest()
            if key in seen:
                continue
            seen.add(key)
            cases.append(
                {
                    "id": f"reg_{key[:12]}",
                    "text": text,
                    "intent_id": seed.get("intent_id"),
                    "period": period,
                    "scope": scope or None,
                    "role": "owner",
                    "expected_status": seed.get("expected_status") or "ok",
                    "seed_id": seed.get("id"),
                }
            )
            if len(cases) >= n:
                return cases[:n]

    # Full combinatorial fill
    i = 0
    while len(cases) < n:
        intent = intents[i % len(intents)]
        period = periods[i % len(periods)]
        scope = scopes[i % len(scopes)]
        role = roles[i % len(roles)]
        status = statuses[i % len(statuses)]
        syns = syn_map.get(intent) or [intent]
        syn = syns[i % len(syns)]
        text = f"{syn} {period} {scope}".strip()
        # Role-sensitive expectation
        expected = status or "ok"
        if intent.startswith("profit.") and role in (
            "tenant_manager",
            "tenant_kiosk",
            "tenant_viewer",
        ):
            expected = "forbidden"
        if intent == "inventory.products":
            expected = "unsupported"
        if intent == "meta.mutation_denied":
            expected = "mutation_denied"
        key = hashlib.sha1(
            f"{intent}|{text}|{role}|{expected}|{i}".encode()
        ).hexdigest()
        if key not in seen:
            seen.add(key)
            cases.append(
                {
                    "id": f"reg_{key[:12]}",
                    "text": text,
                    "intent_id": intent,
                    "period": period,
                    "scope": scope or None,
                    "role": role,
                    "expected_status": expected,
                    "seed_id": None,
                }
            )
        i += 1
        if i > n * 50:
            break
    return cases[:n]


def write_generated(
    *,
    formulations_min: int = 1500,
    regression_n: int = 5000,
) -> dict[str, Any]:
    """Write generated_formulations_v1.json and generated_regression_v1.json."""
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    seeds = load_seeds()
    formulations = generate_formulations(seeds, min_count=formulations_min)
    regression = generate_regression_cases(regression_n, seeds=seeds)
    fpath = _DATA_DIR / "generated_formulations_v1.json"
    rpath = _DATA_DIR / "generated_regression_v1.json"
    fpath.write_text(
        json.dumps(formulations, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    rpath.write_text(
        json.dumps(regression, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    summary = {
        "formulations": len(formulations),
        "regression_cases": len(regression),
        "seeds": len(seeds),
        "paths": {"formulations": str(fpath), "regression": str(rpath)},
    }
    return summary


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Generate Intelligence eval corpora")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write generated_*.json under intelligence/data/",
    )
    parser.add_argument(
        "--count",
        action="store_true",
        help="Print formulation count only",
    )
    parser.add_argument("--regression-n", type=int, default=5000)
    parser.add_argument("--formulations-min", type=int, default=1500)
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.count:
        print(count_formulations())
        return 0
    if args.write:
        summary = write_generated(
            formulations_min=args.formulations_min,
            regression_n=args.regression_n,
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    # default: print counts without write
    seeds = load_seeds()
    print(
        json.dumps(
            {
                "seeds": len(seeds),
                "formulations": count_formulations(seeds),
                "regression_sample": len(
                    generate_regression_cases(min(100, args.regression_n), seeds=seeds)
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
