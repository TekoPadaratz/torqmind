"""Formatação operacional em português brasileiro (Intelligence e carteira)."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional

STATUS_LABELS = {
    "a_vencer": "A vencer",
    "vencido": "Vencido",
    "pago": "Pago",
    "aberto": "Aberto",
}


def format_brl(value: Any) -> str:
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        number = 0.0
    raw = f"{number:,.2f}"
    return "R$ " + raw.replace(",", "X").replace(".", ",").replace("X", ".")


def format_int_br(value: Any) -> str:
    try:
        number = int(value or 0)
    except (TypeError, ValueError):
        number = 0
    return f"{number:,}".replace(",", ".")


def format_date_br(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().strftime("%d/%m/%Y")
    if isinstance(value, date):
        return value.strftime("%d/%m/%Y")
    raw = str(value or "").strip()
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        try:
            return date.fromisoformat(raw[:10]).strftime("%d/%m/%Y")
        except ValueError:
            return raw[:10]
    return raw or "—"


def status_label(value: Any) -> str:
    key = str(value or "").strip()
    return STATUS_LABELS.get(key, key.replace("_", " ").strip().capitalize() or "—")


def filial_display_name(id_empresa: Any, id_filial: Any, fallback: Optional[str] = None) -> str:
    from app.filial_apelido import apelido_for, load_apelido_map

    try:
        fid = int(id_filial)
    except (TypeError, ValueError):
        return fallback or "—"
    try:
        emp = int(id_empresa) if id_empresa is not None else None
    except (TypeError, ValueError):
        emp = None
    apelidos = load_apelido_map(emp) if emp is not None else {}
    label = (apelidos.get(fid) or apelido_for(fid) or fallback or "").strip()
    return label or f"Filial {fid}"
