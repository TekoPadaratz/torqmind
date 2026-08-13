from __future__ import annotations

from typing import Any


SALE_STATUS = 1
CANCELLATION_STATUS = 2
# situacao=3 is NFC-e substitution emission — 100% ignored commercially.
# It is NOT a return/devolução, NOT a sale, NOT a cancellation.
# Must be excluded from all commercial metrics: sales, revenue, cash,
# payments, customers, fraud, goals, dashboard.
IGNORED_BUSINESS_STATUS = 3
# Legacy alias kept only for backward compatibility in tests/imports.
RETURN_STATUS = IGNORED_BUSINESS_STATUS

# Statuses that are commercially valid (for queries that need an allow-list).
COMMERCIAL_STATUSES = (SALE_STATUS, CANCELLATION_STATUS)

# Fora da base comercial de vendas (mesmo com cfop > 5000):
# 5927 = baixa/perda de estoque; 5929/6929 = transferência entre filiais.
SALES_EXCLUDED_CFOPS: tuple[int, ...] = (5927, 5929, 6929)


def sales_status_sql(alias: str) -> str:
    return f"COALESCE({alias}.situacao, 0)"


def sales_status_filter_sql(alias: str, status: int) -> str:
    return f"{sales_status_sql(alias)} = {int(status)}"


def commercial_eligible_sql(alias: str) -> str:
    """Canonical predicate: exclude situacao=3 (ignored business) from commercial queries."""
    return f"{sales_status_sql(alias)} NOT IN ({IGNORED_BUSINESS_STATUS})"


def sales_cfop_filter_sql(alias: str) -> str:
    """Saída comercial: cfop > 5000, sem perda (5927) nem transferência (5929/6929)."""
    excl = ",".join(str(int(c)) for c in SALES_EXCLUDED_CFOPS)
    return (
        f"COALESCE({alias}.cfop, 0) > 5000 "
        f"AND COALESCE({alias}.cfop, 0) NOT IN ({excl})"
    )


def comercial_cfop_numeric_sql(alias: str) -> str:
    return f"etl.cfop_numeric_from_payload({alias}.payload)"


def comercial_cfop_direction_sql(alias: str) -> str:
    return f"etl.cfop_direction({comercial_cfop_numeric_sql(alias)})"


def comercial_cfop_class_sql(alias: str) -> str:
    return f"etl.cfop_commercial_class({comercial_cfop_numeric_sql(alias)})"


def cash_net_value(
    total_vendas: Any,
    total_cancelamentos: Any,
) -> float:
    """Return the explicit cash net value used by Sales/Cash reconciliation.

    Cash net = sales(situacao=1) - cancellations(situacao=2).
    situacao=3 (NFC-e substitution) is excluded entirely — it is not a
    return/devolução and must never affect commercial metrics.
    """

    return round(
        float(total_vendas or 0)
        - float(total_cancelamentos or 0),
        2,
    )
