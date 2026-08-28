"""Período de comissão (21 → 20) e conversão para data_key ClickHouse."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

TZ_SP = ZoneInfo("America/Sao_Paulo")


def today_sp() -> date:
    return datetime.now(TZ_SP).date()


def _add_months(year: int, month: int, delta: int) -> Tuple[int, int]:
    idx = (month - 1) + delta
    return year + idx // 12, (idx % 12) + 1


def default_last_closed_commission_period(ref: Optional[date] = None) -> Tuple[date, date]:
    """Último ciclo 21→20 fechado (referência America/Sao_Paulo).

    Ex.: 15/08 → 21/06–20/07; 28/08 → 21/07–20/08.
    """
    ref = ref or today_sp()
    y, m, d = ref.year, ref.month, ref.day
    if d >= 21:
        start_y, start_m = _add_months(y, m, -1)
        return date(start_y, start_m, 21), date(y, m, 20)
    end_y, end_m = _add_months(y, m, -1)
    start_y, start_m = _add_months(y, m, -2)
    return date(start_y, start_m, 21), date(end_y, end_m, 20)


def parse_iso_date(value: str) -> date:
    text = str(value or "").strip()[:10]
    if len(text) != 10:
        raise ValueError("Data inválida; use AAAA-MM-DD.")
    parsed = date.fromisoformat(text)
    return parsed


def validate_commission_period(dt_ini: date, dt_fim: date) -> None:
    if dt_fim < dt_ini:
        raise ValueError("A data final deve ser igual ou posterior à data inicial.")


def date_to_data_key(d: date) -> int:
    return int(d.year) * 10000 + int(d.month) * 100 + int(d.day)


def data_key_bounds_inclusive(dt_ini: date, dt_fim: date) -> Tuple[int, int]:
    """Retorna (dk_ini, dk_fim) inclusivos para ``BETWEEN`` no slim."""
    validate_commission_period(dt_ini, dt_fim)
    return date_to_data_key(dt_ini), date_to_data_key(dt_fim)


def data_key_bounds_half_open(dt_ini: date, dt_fim: date) -> Tuple[int, int]:
    """Retorna (dk_ini, dk_fim_exclusivo) para ``>= ini AND < fim``."""
    validate_commission_period(dt_ini, dt_fim)
    dk_ini = date_to_data_key(dt_ini)
    dk_fim_excl = date_to_data_key(dt_fim + timedelta(days=1))
    return dk_ini, dk_fim_excl


def resolve_commission_period_from_query(
    dt_ini: Optional[str] = None,
    dt_fim: Optional[str] = None,
) -> Tuple[date, date]:
    """Resolve período da query; default = último ciclo 21→20 fechado."""
    if dt_ini and dt_fim:
        start = parse_iso_date(dt_ini)
        end = parse_iso_date(dt_fim)
        validate_commission_period(start, end)
        return start, end
    if dt_ini or dt_fim:
        raise ValueError("Informe dt_ini e dt_fim.")
    return default_last_closed_commission_period()
