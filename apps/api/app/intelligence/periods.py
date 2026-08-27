"""Parser de períodos pt-BR (timezone America/Sao_Paulo)."""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

from app.business_time import business_today
from app.intelligence.normalize import fold_key


_MONTHS = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
    "jan": 1,
    "fev": 2,
    "mar": 3,
    "abr": 4,
    "mai": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "set": 9,
    "out": 10,
    "nov": 11,
    "dez": 12,
}


@dataclass(frozen=True)
class PeriodResult:
    dt_ini: date
    dt_fim: date
    label: str
    ambiguous_year: bool = False


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    last = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last)


def _parse_br_date(token: str) -> Optional[date]:
    token = token.strip()
    m = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?", token)
    if not m:
        return None
    d, mo, y = int(m.group(1)), int(m.group(2)), m.group(3)
    today = business_today()
    year = today.year if y is None else int(y)
    if year < 100:
        year += 2000
    try:
        return date(year, mo, d)
    except ValueError:
        return None


def parse_period(text: str | None, *, as_of: date | None = None) -> Optional[PeriodResult]:
    """Extrai período; nunca inventa 1970. Retorna None se não houver período claro."""
    today = as_of or business_today()
    key = fold_key(text)
    if not key:
        return None

    if re.search(r"\bhoje\b", key):
        return PeriodResult(today, today, "hoje")

    if re.search(r"\bontem\b", key):
        d = today - timedelta(days=1)
        return PeriodResult(d, d, "ontem")

    if re.search(r"\banteontem\b", key):
        d = today - timedelta(days=2)
        return PeriodResult(d, d, "anteontem")

    if re.search(r"\b(esta semana|essa semana|semana atual)\b", key):
        start = today - timedelta(days=today.weekday())
        return PeriodResult(start, today, "esta semana")

    m = re.search(r"\bultimos?\s+(\d{1,3})\s+dias?\b", key)
    if m:
        n = max(1, min(366, int(m.group(1))))
        start = today - timedelta(days=n - 1)
        return PeriodResult(start, today, f"últimos {n} dias")

    if re.search(r"\b(mes atual|este mes|mes corrente)\b", key):
        ini, fim = _month_bounds(today.year, today.month)
        return PeriodResult(ini, min(fim, today), "mês atual")

    if re.search(r"\bmes passado\b|\bultimo mes\b", key):
        month = today.month - 1 or 12
        year = today.year if today.month > 1 else today.year - 1
        ini, fim = _month_bounds(year, month)
        return PeriodResult(ini, fim, "mês passado")

    if re.search(r"\b(acumulado do ano|ano atual|ytd|este ano)\b", key):
        return PeriodResult(date(today.year, 1, 1), today, "acumulado do ano")

    m = re.search(r"\bde\s+(\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)\s+ate\s+(\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)\b", key)
    if m:
        a = _parse_br_date(m.group(1))
        b = _parse_br_date(m.group(2))
        if a and b and a.year >= 2000 and b.year >= 2000:
            if a > b:
                a, b = b, a
            return PeriodResult(a, b, f"de {a.isoformat()} até {b.isoformat()}")

    for name, month in _MONTHS.items():
        # "agosto passado"
        if re.search(rf"\b{name}\s+passado\b", key):
            year = today.year if today.month > month else today.year - 1
            if today.month == month:
                year = today.year - 1
            ini, fim = _month_bounds(year, month)
            return PeriodResult(ini, fim, f"{name} passado", ambiguous_year=False)

        # "agosto 2025" / "agosto/2025"
        m = re.search(rf"\b{name}\s*(?:/|\s+)?(20\d{{2}})\b", key)
        if m:
            year = int(m.group(1))
            ini, fim = _month_bounds(year, month)
            return PeriodResult(ini, fim, f"{name}/{year}")

        # "agosto" sozinho — ano corrente, mas marca ambiguidade se ainda estamos no mês
        if re.search(rf"\b{name}\b", key):
            year = today.year
            # se o mês ainda não começou neste ano, usa ano passado
            if month > today.month:
                year = today.year - 1
            ini, fim = _month_bounds(year, month)
            ambiguous = month != today.month or True  # mês nomeado sem ano = pedir confirmação suave
            # Para "agosto" no meio do ano: ano atual é razoável; ainda assim sinaliza ambiguous_year
            return PeriodResult(ini, min(fim, today) if year == today.year else fim, name, ambiguous_year=True)

    return None


def default_period(*, as_of: date | None = None) -> PeriodResult:
    today = as_of or business_today()
    ini, _ = _month_bounds(today.year, today.month)
    return PeriodResult(ini, today, "mês atual")
