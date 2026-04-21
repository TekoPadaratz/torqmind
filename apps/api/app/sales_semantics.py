from __future__ import annotations

from typing import Any


SALE_STATUS = 1
CANCELLATION_STATUS = 2
RETURN_STATUS = 3


def sales_status_sql(alias: str) -> str:
    return f"COALESCE({alias}.situacao, 0)"


def sales_status_filter_sql(alias: str, status: int) -> str:
    return f"{sales_status_sql(alias)} = {int(status)}"


def sales_cfop_filter_sql(alias: str) -> str:
    return f"COALESCE({alias}.cfop, 0) > 5000"


def comercial_cfop_numeric_sql(alias: str) -> str:
    return f"etl.cfop_numeric_from_payload({alias}.payload)"


def comercial_cfop_direction_sql(alias: str) -> str:
    return f"etl.cfop_direction({comercial_cfop_numeric_sql(alias)})"


def comercial_cfop_class_sql(alias: str) -> str:
    return f"etl.cfop_commercial_class({comercial_cfop_numeric_sql(alias)})"


def cash_net_value(
    total_vendas: Any,
    total_cancelamentos: Any,
    total_devolucoes: Any,
) -> float:
    """Return the explicit cash net value used by Sales/Cash reconciliation.

    The operational cash net in this package is intentionally sales-status based:
    sale(situacao=1) - cancellation(situacao=2) - return(situacao=3).
    Payment receipts remain exposed separately because settlement timing is not the
    same semantic as commercial sales recognition.
    """

    return round(
        float(total_vendas or 0)
        - float(total_cancelamentos or 0)
        - float(total_devolucoes or 0),
        2,
    )
