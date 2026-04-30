"""Table mappings: Debezium topic → ClickHouse current table and column definitions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class TableMapping:
    """Mapping from a Debezium source table to ClickHouse current table."""

    source_schema: str
    source_table: str
    ch_database: str
    ch_table: str
    primary_key: tuple[str, ...]
    # Columns to extract from Debezium payload (order matters for inserts)
    columns: tuple[str, ...]


# All table mappings for the first CDC scope
TABLE_MAPPINGS: dict[str, TableMapping] = {}


def _register(m: TableMapping) -> None:
    key = f"{m.source_schema}.{m.source_table}"
    TABLE_MAPPINGS[key] = m


# ---------- Dimensions ----------

_register(TableMapping(
    source_schema="dw", source_table="dim_filial",
    ch_database="torqmind_current", ch_table="dim_filial",
    primary_key=("id_empresa", "id_filial"),
    columns=("id_empresa", "id_filial", "nome", "cnpj", "razao_social"),
))

_register(TableMapping(
    source_schema="dw", source_table="dim_produto",
    ch_database="torqmind_current", ch_table="dim_produto",
    primary_key=("id_empresa", "id_filial", "id_produto"),
    columns=("id_empresa", "id_filial", "id_produto", "nome", "unidade",
             "id_grupo_produto", "id_local_venda", "custo_medio"),
))

_register(TableMapping(
    source_schema="dw", source_table="dim_grupo_produto",
    ch_database="torqmind_current", ch_table="dim_grupo_produto",
    primary_key=("id_empresa", "id_filial", "id_grupo_produto"),
    columns=("id_empresa", "id_filial", "id_grupo_produto", "nome"),
))

_register(TableMapping(
    source_schema="dw", source_table="dim_funcionario",
    ch_database="torqmind_current", ch_table="dim_funcionario",
    primary_key=("id_empresa", "id_filial", "id_funcionario"),
    columns=("id_empresa", "id_filial", "id_funcionario", "nome"),
))

_register(TableMapping(
    source_schema="dw", source_table="dim_usuario_caixa",
    ch_database="torqmind_current", ch_table="dim_usuario_caixa",
    primary_key=("id_empresa", "id_filial", "id_usuario"),
    columns=("id_empresa", "id_filial", "id_usuario", "nome", "payload"),
))

_register(TableMapping(
    source_schema="dw", source_table="dim_local_venda",
    ch_database="torqmind_current", ch_table="dim_local_venda",
    primary_key=("id_empresa", "id_filial", "id_local_venda"),
    columns=("id_empresa", "id_filial", "id_local_venda", "nome"),
))

_register(TableMapping(
    source_schema="dw", source_table="dim_cliente",
    ch_database="torqmind_current", ch_table="dim_cliente",
    primary_key=("id_empresa", "id_filial", "id_cliente"),
    columns=("id_empresa", "id_filial", "id_cliente", "nome", "documento"),
))

# ---------- Facts ----------

_register(TableMapping(
    source_schema="dw", source_table="fact_venda",
    ch_database="torqmind_current", ch_table="fact_venda",
    primary_key=("id_empresa", "id_filial", "id_db", "id_movprodutos"),
    columns=("id_empresa", "id_filial", "id_db", "id_movprodutos", "data_key",
             "id_usuario", "id_cliente", "id_comprovante", "id_turno",
             "saidas_entradas", "total_venda", "cancelado"),
))

_register(TableMapping(
    source_schema="dw", source_table="fact_venda_item",
    ch_database="torqmind_current", ch_table="fact_venda_item",
    primary_key=("id_empresa", "id_filial", "id_db", "id_movprodutos", "id_itensmovprodutos"),
    columns=("id_empresa", "id_filial", "id_db", "id_movprodutos", "id_itensmovprodutos",
             "data_key", "id_produto", "id_grupo_produto", "id_local_venda",
             "id_funcionario", "cfop", "qtd", "valor_unitario", "total",
             "desconto", "custo_total", "margem"),
))

_register(TableMapping(
    source_schema="dw", source_table="fact_comprovante",
    ch_database="torqmind_current", ch_table="fact_comprovante",
    primary_key=("id_empresa", "id_filial", "id_db", "id_comprovante"),
    columns=("id_empresa", "id_filial", "id_db", "id_comprovante", "data_key",
             "id_usuario", "id_turno", "id_cliente", "valor_total", "cancelado", "situacao"),
))

_register(TableMapping(
    source_schema="dw", source_table="fact_pagamento_comprovante",
    ch_database="torqmind_current", ch_table="fact_pagamento_comprovante",
    primary_key=("id_empresa", "id_filial", "referencia", "tipo_forma"),
    columns=("id_empresa", "id_filial", "referencia", "id_db", "id_comprovante",
             "id_turno", "id_usuario", "tipo_forma", "valor", "dt_evento",
             "data_key", "nsu", "autorizacao", "bandeira", "rede", "tef"),
))

_register(TableMapping(
    source_schema="dw", source_table="fact_caixa_turno",
    ch_database="torqmind_current", ch_table="fact_caixa_turno",
    primary_key=("id_empresa", "id_filial", "id_turno"),
    columns=("id_empresa", "id_filial", "id_turno", "id_db", "id_usuario",
             "abertura_ts", "fechamento_ts", "data_key_abertura", "data_key_fechamento",
             "encerrante_fechamento", "is_aberto", "status_raw"),
))

_register(TableMapping(
    source_schema="dw", source_table="fact_financeiro",
    ch_database="torqmind_current", ch_table="fact_financeiro",
    primary_key=("id_empresa", "id_filial", "id_db", "tipo_titulo", "id_titulo"),
    columns=("id_empresa", "id_filial", "id_db", "tipo_titulo", "id_titulo",
             "id_entidade", "data_emissao", "data_key_emissao", "vencimento",
             "data_key_venc", "data_pagamento", "data_key_pgto", "valor", "valor_pago"),
))

_register(TableMapping(
    source_schema="dw", source_table="fact_risco_evento",
    ch_database="torqmind_current", ch_table="fact_risco_evento",
    primary_key=("id_empresa", "id_filial", "id"),
    columns=("id", "id_empresa", "id_filial", "data_key", "event_type", "source",
             "id_db", "id_comprovante", "id_movprodutos", "id_usuario",
             "id_funcionario", "id_turno", "id_cliente", "valor_total",
             "impacto_estimado", "score_risco", "score_level", "reasons"),
))

# ---------- App / Config ----------

_register(TableMapping(
    source_schema="app", source_table="payment_type_map",
    ch_database="torqmind_current", ch_table="payment_type_map",
    primary_key=("id",),
    columns=("id", "id_empresa", "tipo_forma", "label", "category",
             "severity_hint", "active"),
))


def get_mapping(schema: str, table: str) -> TableMapping | None:
    """Lookup table mapping by schema.table."""
    return TABLE_MAPPINGS.get(f"{schema}.{table}")
