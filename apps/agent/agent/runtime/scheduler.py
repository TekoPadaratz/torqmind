"""Dataset scheduling tiers — reduce SQL Server load by skipping cold/warm datasets."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

# Period = run every N cycles (1 = every cycle).
DEFAULT_TIER_PERIODS: Dict[str, int] = {
    "hot": 1,
    "warm": 5,
    "cold": 15,
}

DEFAULT_DATASET_TIERS: Dict[str, str] = {
    # hot — commercial realtime
    "comprovantes": "hot",
    "itenscomprovantes": "hot",
    "formas_pgto_comprovantes": "hot",
    "turnos": "hot",
    "nfe": "hot",
    "usuarios": "hot",
    # warm — finance / movements
    "contasreceber": "warm",
    "contasreceberbaixa": "warm",
    "contaspagar": "warm",
    "contaspagarbaixa": "warm",
    "movbancos": "warm",
    "movlctos": "warm",
    "movlctoscancelados": "warm",
    "cheques": "warm",
    "estoque": "warm",
    # cold — dimensions / slow-changing
    "entidades": "cold",
    "clientes": "cold",
    "produtos": "cold",
    "grupoprodutos": "cold",
    "planodecontas": "cold",
    "filiais": "cold",
    "funcionarios": "cold",
    "localvendas": "cold",
    "formaspagamento": "cold",
}


def tier_for_dataset(dataset: str, ds_cfg: Optional[Dict[str, Any]] = None) -> str:
    cfg = ds_cfg or {}
    explicit = str(cfg.get("tier") or "").strip().lower()
    if explicit in DEFAULT_TIER_PERIODS:
        return explicit
    return DEFAULT_DATASET_TIERS.get(dataset, "warm")


def period_for_tier(tier: str, runtime_cfg: Optional[Dict[str, Any]] = None) -> int:
    runtime = runtime_cfg or {}
    overrides = runtime.get("tier_periods") if isinstance(runtime.get("tier_periods"), dict) else {}
    if tier in overrides:
        try:
            return max(1, int(overrides[tier]))
        except (TypeError, ValueError):
            pass
    return max(1, int(DEFAULT_TIER_PERIODS.get(tier, 5)))


def should_run_dataset(
    dataset: str,
    cycle_index: int,
    *,
    ds_cfg: Optional[Dict[str, Any]] = None,
    runtime_cfg: Optional[Dict[str, Any]] = None,
    force: bool = False,
) -> bool:
    """Return True if this dataset should run on the given 1-based cycle index."""
    if force:
        return True
    if cycle_index < 1:
        cycle_index = 1
    tier = tier_for_dataset(dataset, ds_cfg)
    period = period_for_tier(tier, runtime_cfg)
    return (cycle_index % period) == 0


def filter_datasets_for_cycle(
    datasets: Iterable[str],
    cycle_index: int,
    *,
    cfg_datasets: Dict[str, Dict[str, Any]],
    runtime_cfg: Optional[Dict[str, Any]] = None,
    force_all: bool = False,
    only_dataset: Optional[str] = None,
) -> List[str]:
    if only_dataset:
        return [only_dataset] if only_dataset in cfg_datasets or only_dataset else [only_dataset]
    out: List[str] = []
    for name in datasets:
        ds_cfg = cfg_datasets.get(name) or {}
        if not ds_cfg.get("enabled", True):
            continue
        if should_run_dataset(
            name,
            cycle_index,
            ds_cfg=ds_cfg,
            runtime_cfg=runtime_cfg,
            force=force_all,
        ):
            out.append(name)
    return out
