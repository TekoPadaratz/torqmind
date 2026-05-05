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
             "data", "id_usuario", "id_cliente", "id_comprovante", "id_turno",
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

# ---------- STG canonical source ----------

_STG_COMMON = (
    "payload",
    "ingested_at",
    "dt_evento",
    "id_db_shadow",
    "id_chave_natural",
    "received_at",
)


def _stg_columns(*pk: str, extra: tuple[str, ...] = ()) -> tuple[str, ...]:
    return (*pk, *_STG_COMMON, *extra)


_register(TableMapping(
    source_schema="stg", source_table="filiais",
    ch_database="torqmind_current", ch_table="stg_filiais",
    primary_key=("id_empresa", "id_filial"),
    columns=_stg_columns("id_empresa", "id_filial"),
))

_register(TableMapping(
    source_schema="stg", source_table="funcionarios",
    ch_database="torqmind_current", ch_table="stg_funcionarios",
    primary_key=("id_empresa", "id_filial", "id_funcionario"),
    columns=_stg_columns("id_empresa", "id_filial", "id_funcionario"),
))

_register(TableMapping(
    source_schema="stg", source_table="usuarios",
    ch_database="torqmind_current", ch_table="stg_usuarios",
    primary_key=("id_empresa", "id_filial", "id_usuario"),
    columns=_stg_columns("id_empresa", "id_filial", "id_usuario"),
))

_register(TableMapping(
    source_schema="stg", source_table="entidades",
    ch_database="torqmind_current", ch_table="stg_entidades",
    primary_key=("id_empresa", "id_filial", "id_entidade"),
    columns=_stg_columns("id_empresa", "id_filial", "id_entidade"),
))

# Future-proof mapping for deployments that add a physical stg.clientes table.
# The current API ingest aliases clientes into stg.entidades.
_register(TableMapping(
    source_schema="stg", source_table="clientes",
    ch_database="torqmind_current", ch_table="stg_clientes",
    primary_key=("id_empresa", "id_filial", "id_cliente"),
    columns=_stg_columns("id_empresa", "id_filial", "id_cliente"),
))

_register(TableMapping(
    source_schema="stg", source_table="grupoprodutos",
    ch_database="torqmind_current", ch_table="stg_grupoprodutos",
    primary_key=("id_empresa", "id_filial", "id_grupoprodutos"),
    columns=_stg_columns("id_empresa", "id_filial", "id_grupoprodutos"),
))

_register(TableMapping(
    source_schema="stg", source_table="localvendas",
    ch_database="torqmind_current", ch_table="stg_localvendas",
    primary_key=("id_empresa", "id_filial", "id_localvendas"),
    columns=_stg_columns("id_empresa", "id_filial", "id_localvendas"),
))

_register(TableMapping(
    source_schema="stg", source_table="produtos",
    ch_database="torqmind_current", ch_table="stg_produtos",
    primary_key=("id_empresa", "id_filial", "id_produto"),
    columns=_stg_columns("id_empresa", "id_filial", "id_produto"),
))

_register(TableMapping(
    source_schema="stg", source_table="turnos",
    ch_database="torqmind_current", ch_table="stg_turnos",
    primary_key=("id_empresa", "id_filial", "id_turno"),
    columns=_stg_columns("id_empresa", "id_filial", "id_turno"),
))

_register(TableMapping(
    source_schema="stg", source_table="comprovantes",
    ch_database="torqmind_current", ch_table="stg_comprovantes",
    primary_key=("id_empresa", "id_filial", "id_db", "id_comprovante"),
    columns=_stg_columns(
        "id_empresa", "id_filial", "id_db", "id_comprovante",
        extra=(
            "referencia_shadow", "id_usuario_shadow", "id_turno_shadow",
            "id_cliente_shadow", "valor_total_shadow", "cancelado_shadow",
            "situacao_shadow",
        ),
    ),
))

_register(TableMapping(
    source_schema="stg", source_table="itenscomprovantes",
    ch_database="torqmind_current", ch_table="stg_itenscomprovantes",
    primary_key=("id_empresa", "id_filial", "id_db", "id_comprovante", "id_itemcomprovante"),
    columns=_stg_columns(
        "id_empresa", "id_filial", "id_db", "id_comprovante", "id_itemcomprovante",
        extra=(
            "id_produto_shadow", "id_grupo_produto_shadow", "id_local_venda_shadow",
            "id_funcionario_shadow", "cfop_shadow", "qtd_shadow",
            "valor_unitario_shadow", "total_shadow", "desconto_shadow",
            "custo_unitario_shadow",
        ),
    ),
))

_register(TableMapping(
    source_schema="stg", source_table="formas_pgto_comprovantes",
    ch_database="torqmind_current", ch_table="stg_formas_pgto_comprovantes",
    primary_key=("id_empresa", "id_filial", "id_referencia", "tipo_forma"),
    columns=_stg_columns(
        "id_empresa", "id_filial", "id_referencia", "tipo_forma",
        extra=(
            "valor_shadow", "nsu_shadow", "autorizacao_shadow", "bandeira_shadow",
            "rede_shadow", "tef_shadow",
        ),
    ),
))

_register(TableMapping(
    source_schema="stg", source_table="contaspagar",
    ch_database="torqmind_current", ch_table="stg_contaspagar",
    primary_key=("id_empresa", "id_filial", "id_db", "id_contaspagar"),
    columns=_stg_columns("id_empresa", "id_filial", "id_db", "id_contaspagar"),
))

_register(TableMapping(
    source_schema="stg", source_table="contasreceber",
    ch_database="torqmind_current", ch_table="stg_contasreceber",
    primary_key=("id_empresa", "id_filial", "id_db", "id_contasreceber"),
    columns=_stg_columns("id_empresa", "id_filial", "id_db", "id_contasreceber"),
))

_register(TableMapping(
    source_schema="stg", source_table="financeiro",
    ch_database="torqmind_current", ch_table="stg_financeiro",
    primary_key=("id_empresa", "id_filial", "id_db", "tipo_titulo", "id_titulo"),
    columns=_stg_columns("id_empresa", "id_filial", "id_db", "tipo_titulo", "id_titulo"),
))


# ---------- App / Config ----------

_register(TableMapping(
    source_schema="app", source_table="payment_type_map",
    ch_database="torqmind_current", ch_table="payment_type_map",
    primary_key=("id",),
    columns=("id", "id_empresa", "tipo_forma", "label", "category",
             "severity_hint", "active"),
))

_register(TableMapping(
    source_schema="app", source_table="goals",
    ch_database="torqmind_current", ch_table="goals",
    primary_key=("id",),
    columns=("id", "id_empresa", "id_filial", "goal_date", "goal_type",
             "target_value", "created_at"),
))


def get_mapping(schema: str, table: str) -> TableMapping | None:
    """Lookup table mapping by schema.table."""
    return TABLE_MAPPINGS.get(f"{schema}.{table}")
