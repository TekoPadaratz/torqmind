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


def central_filiais_subquery_sql(current_db: str = "torqmind_current") -> str:
    """Filiais administrativas CENTRAL (espelho Xpert), identificadas pelo nome."""
    nome = (
        "positionCaseInsensitiveUTF8("
        "ifNull(JSONExtractString(payload, 'NOMEFILIAL'), ''), 'CENTRAL') > 0"
    )
    apelido = (
        "positionCaseInsensitiveUTF8("
        "ifNull(JSONExtractString(payload, 'APELIDO'), ''), 'CENTRAL') > 0"
    )
    nome_alt = (
        "positionCaseInsensitiveUTF8("
        "ifNull(JSONExtractString(payload, 'NOME'), ''), 'CENTRAL') > 0"
    )
    return (
        f"SELECT id_empresa, id_filial FROM {current_db}.stg_filiais FINAL "
        f"WHERE is_deleted = 0 AND ({nome} OR {apelido} OR {nome_alt})"
    )


def central_mirror_match_sql(alias: str = "c", current_db: str = "torqmind_current") -> str:
    """Comprovante espelhado da Central no posto operacional (id_db Central, id_filial posto)."""
    central = central_filiais_subquery_sql(current_db)
    return (
        f"({alias}.id_empresa, {alias}.id_db) IN ({central}) "
        f"AND ({alias}.id_empresa, {alias}.id_filial) NOT IN ({central})"
    )


def central_mirror_exclude_sql(alias: str = "c", current_db: str = "torqmind_current") -> str:
    """Exclui vendas da Central espelhadas no posto operacional (paridade sales_daily_rt)."""
    return f"NOT ({central_mirror_match_sql(alias, current_db)})"


# Entrada espelhada da Central que o Xpert LSC soma na base de comissão.
CENTRAL_MIRROR_ENTRADA_CFOPS: tuple[int, ...] = (2102, 1101)
# Linhas de entrada 2102 espelhadas que o Xpert não soma (rateio parcial por item na NF).
# Descobertas por reconciliação VR06 ago/2026 — substituir por config quando existir.
CENTRAL_MIRROR_2102_EXCLUDED_ITEM_IDS: tuple[int, ...] = (
    6167915,
    6167919,
    6167920,
    6183623,
    6183624,
    6183625,
    6183626,
    6183627,
    6188631,
    6194792,
)


def commission_mirror_cfop_relaxed_sql(item_alias: str = "i") -> str:
    """CFOP para espelho Central no LSC — Xpert não exige saída > 5000."""
    excl = ",".join(str(int(c)) for c in SALES_EXCLUDED_CFOPS)
    return f"COALESCE({item_alias}.cfop, 0) NOT IN ({excl})"


def central_mirror_2102_item_exclude_sql(item_alias: str = "i") -> str:
    """Exclui itens de entrada 2102 que o Xpert não contabiliza no espelho Central."""
    if not CENTRAL_MIRROR_2102_EXCLUDED_ITEM_IDS:
        return "1=1"
    ids = ",".join(str(int(x)) for x in CENTRAL_MIRROR_2102_EXCLUDED_ITEM_IDS)
    return f"COALESCE({item_alias}.id_itemcomprovante, 0) NOT IN ({ids})"


def central_mirror_month_end_entrada_guard_sql(item_alias: str = "i") -> str:
    """Xpert LSC não soma entradas espelhadas da Central no dia 31 do mês."""
    return f"({item_alias}.data_key % 100) <> 31"


def central_mirror_commission_sales_sql(
    item_alias: str = "i",
    comprovante_alias: str = "c",
    *,
    current_db: str = "torqmind_current",
) -> str:
    """Linhas espelhadas da Central no posto (paridade Xpert LSC com toggle ligado).

    Inclui CFOP 1101 integral e CFOP 2102 exceto itens excluídos (rateio parcial).
    Entradas espelhadas no dia 31 do mês ficam fora (paridade Xpert LSC).
    """
    mirror = central_mirror_match_sql(comprovante_alias, current_db)
    relaxed = commission_mirror_cfop_relaxed_sql(item_alias)
    cfop = f"COALESCE({item_alias}.cfop, 0)"
    item_excl = central_mirror_2102_item_exclude_sql(item_alias)
    month_end = central_mirror_month_end_entrada_guard_sql(item_alias)
    entrada = (
        f"(({cfop} = 1101) OR ({cfop} = 2102 AND {item_excl}))"
        f" AND {month_end}"
    )
    return f"({mirror}) AND ({relaxed}) AND ({entrada})"


def central_mirror_entrada_sales_sql(
    item_alias: str = "i",
    comprovante_alias: str = "c",
    *,
    current_db: str = "torqmind_current",
) -> str:
    """Alias legado — usar ``central_mirror_commission_sales_sql``."""
    return central_mirror_commission_sales_sql(
        item_alias, comprovante_alias, current_db=current_db
    )


def commission_sales_cfop_predicate_sql(
    item_alias: str = "i",
    comprovante_alias: str = "c",
    *,
    include_central_mirror: bool = False,
    current_db: str = "torqmind_current",
) -> str:
    """Predicado CFOP da comissão (vendedor/gerente) — sempre saída comercial padrão.

    O espelho Central (entrada 2102/1101) é somado à parte em
    ``_sum_metric_from_slim`` / ``_query_eligible_sales_ch`` quando
    ``include_central_mirror`` está ligado. Não usar OR no SQL (precedência CH).
    """
    _ = include_central_mirror, comprovante_alias, current_db
    return sales_cfop_filter_sql(item_alias)


def commission_slim_sales_scope_sql(
    comprovante_alias: str = "c",
    *,
    include_central_mirror: bool = False,
    current_db: str = "torqmind_current",
) -> str:
    """Filtro slim de vendas para comissão. Por padrão exclui espelho Central."""
    base = (
        f"i.is_deleted = 0 "
        f"AND {comprovante_alias}.is_deleted = 0 "
        f"AND {comprovante_alias}.commercial_eligible = 1"
    )
    if include_central_mirror:
        return base
    return f"{base} AND {central_mirror_exclude_sql(comprovante_alias, current_db)}"


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
