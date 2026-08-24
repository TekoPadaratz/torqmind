from __future__ import annotations

"""High-throughput ingestion endpoint (NDJSON).

PT-BR:
- O cliente (agent/extractor) faz POST de NDJSON (1 JSON por linha).
- A API faz UPSERT em `stg.*` (raw JSONB) com PK composta.
- Opcional: dispara ETL (STG->DW->MART) logo após ingest.

EN:
- Client posts NDJSON (1 JSON per line).
- API upserts into `stg.*` raw tables with composite PK.
- Optional: trigger ETL after ingest.

Why NDJSON?
- Streaming-friendly
- Simple to generate from SQL Server
"""

import asyncio
import json
import logging
import zlib
from datetime import date, datetime, timedelta
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple
from uuid import uuid4

from fastapi import APIRouter, Header, HTTPException, Query, Request
from psycopg import sql

from app.business_time import business_today, business_date_for_datetime, coerce_operational_datetime
from app.config import settings
from app.db import get_conn
from app.services.etl_orchestrator import EtlCycleBusyError, TRACK_OPERATIONAL, run_incremental_cycle
from app.services.telegram import notify_cancelled_comprovantes, notify_voided_nfes, raw_comprovante_is_cancelled, raw_nfe_is_voided
from app.services.alert_preco_fixo import notify_preco_fixo_from_itens

router = APIRouter(prefix="/ingest", tags=["ingest"])
logger = logging.getLogger(__name__)

# Janela curta de vendas comerciais. NÃO incluir movprodutos/itensmovprodutos:
# essas tabelas são movimentação de estoque (entrada/saída loja e combustível)
# e precisam acompanhar o bootstrap longo do agent (SOLVENCIA_BOOTSTRAP_DAYS).
# Incluir movprodutos aqui rejeitava cabeçalhos antigos e gerava órfãos em
# stg.itensmovprodutos (itens sem header) — quebra estoque e entradas de combustível.
SALES_RETENTION_DATASETS = {"comprovantes"}


def _get_any(d: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        try:
            # handle numeric strings with spaces
            return int(str(x).strip())
        except Exception:
            return None


def _parse_ts(x: Any, tenant_id: int | None = None) -> Optional[datetime]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return coerce_operational_datetime(x, tenant_id)
    raw = str(x).strip()
    if not raw:
        return None
    candidates = [
        raw,
        raw.replace(" ", "T"),
    ]
    for c in candidates:
        try:
            dt = datetime.fromisoformat(c.replace("Z", "+00:00"))
            return coerce_operational_datetime(dt, tenant_id)
        except Exception:
            pass
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(raw, fmt)
            return coerce_operational_datetime(dt, tenant_id)
        except Exception:
            continue
    return None


def _to_numeric(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        raw = str(x).strip().replace(",", ".")
        if not raw:
            return None
        try:
            return float(raw)
        except Exception:
            return None


def _to_bool(x: Any) -> Optional[bool]:
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    raw = str(x).strip().lower()
    if raw in {"1", "true", "t", "yes", "y", "sim", "s"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "nao", "não"}:
        return False
    return None


def _strip_null_chars(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, dict):
        return {key: _strip_null_chars(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_strip_null_chars(item) for item in value]
    return value


def _infer_dt_evento(obj: Dict[str, Any], tenant_id: int | None = None) -> Optional[datetime]:
    keys = [
        "TORQMIND_DT_EVENTO",
        "DT_EVENTO",
        "DATA",
        "DATAMOV",
        "DTMOV",
        "DTALTERACAO",
        "DTCADASTRO",
        "VENCIMENTO",
    ]
    for k in keys:
        if k in obj:
            dt = _parse_ts(obj.get(k), tenant_id=tenant_id)
            if dt is not None:
                return dt
    return None


def _infer_id_db_shadow(obj: Dict[str, Any]) -> Optional[int]:
    return _to_int(_get_any(obj, ["ID_DB", "id_db", "ID", "id"]))


def _infer_natural_key(obj: Dict[str, Any], pk_values: Dict[str, Any]) -> str:
    if "ID_CHAVE_NATURAL" in obj and obj["ID_CHAVE_NATURAL"] not in (None, ""):
        return str(obj["ID_CHAVE_NATURAL"])
    parts = [f"{k}={pk_values.get(k)}" for k in sorted(pk_values.keys()) if pk_values.get(k) is not None]
    if parts:
        return "|".join(parts)
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)[:1000]


def _shadow_values_for_dataset(dataset_key: str, obj: Dict[str, Any]) -> Dict[str, Any]:
    if dataset_key == "comprovantes":
        return {
            "referencia_shadow": _to_int(_get_any(obj, ["REFERENCIA", "referencia", "ID_REFERENCIA", "id_referencia"])),
            "id_usuario_shadow": _to_int(_get_any(obj, ["ID_USUARIOS", "ID_USUARIO", "id_usuario"])),
            "id_turno_shadow": _to_int(_get_any(obj, ["ID_TURNOS", "ID_TURNO", "id_turno"])),
            "id_cliente_shadow": _to_int(_get_any(obj, ["ID_ENTIDADE", "ID_CLIENTE", "id_entidade", "id_cliente"])),
            "valor_total_shadow": _to_numeric(_get_any(obj, ["VLRTOTAL", "VLR_TOTAL", "valor_total"])),
            "cancelado_shadow": _to_bool(_get_any(obj, ["CANCELADO", "cancelado"])),
            "situacao_shadow": _to_int(_get_any(obj, ["SITUACAO", "situacao", "STATUS", "status"])),
        }
    if dataset_key == "movprodutos":
        return {
            "id_comprovante_shadow": _to_int(_get_any(obj, ["ID_COMPROVANTE", "id_comprovante"])),
            "id_usuario_shadow": _to_int(_get_any(obj, ["ID_USUARIOS", "ID_USUARIO", "id_usuario"])),
            "id_turno_shadow": _to_int(_get_any(obj, ["ID_TURNOS", "ID_TURNO", "id_turno"])),
            "id_cliente_shadow": _to_int(_get_any(obj, ["ID_ENTIDADE", "ID_CLIENTE", "id_entidade", "id_cliente"])),
            "saidas_entradas_shadow": _to_int(_get_any(obj, ["SAIDAS_ENTRADAS", "saidas_entradas"])),
            "total_venda_shadow": _to_numeric(_get_any(obj, ["TOTALVENDA", "TOTAL_VENDA", "total_venda"])),
            "situacao_shadow": _to_int(_get_any(obj, ["SITUACAO", "situacao", "STATUS", "status"])),
        }
    if dataset_key == "itensmovprodutos":
        return {
            "id_produto_shadow": _to_int(_get_any(obj, ["ID_PRODUTOS", "ID_PRODUTO", "id_produto"])),
            "id_grupo_produto_shadow": _to_int(_get_any(obj, ["ID_GRUPOPRODUTOS", "ID_GRUPO_PRODUTO", "id_grupoprodutos"])),
            "id_local_venda_shadow": _to_int(_get_any(obj, ["ID_LOCALVENDAS", "ID_LOCAL_VENDA", "id_localvendas"])),
            "id_funcionario_shadow": _to_int(_get_any(obj, ["ID_FUNCIONARIOS", "ID_FUNCIONARIO", "id_funcionario"])),
            "cfop_shadow": _to_int(_get_any(obj, ["CFOP", "cfop"])),
            "qtd_shadow": _to_numeric(_get_any(obj, ["QTDE", "QTD", "quantidade"])),
            "valor_unitario_shadow": _to_numeric(_get_any(obj, ["VLRUNITARIO", "VALOR_UNITARIO", "valor_unitario"])),
            "total_shadow": _to_numeric(_get_any(obj, ["TOTAL", "VLRTOTALITEM", "VLRTOTAL", "total"])),
            "desconto_shadow": _to_numeric(_get_any(obj, ["VLRDESCONTO", "VALOR_DESCONTO", "desconto"])),
            "custo_unitario_shadow": _to_numeric(
                _get_any(
                    obj,
                    [
                        "VLRCUSTOCOMICMS",
                        "VLR_CUSTO_COM_ICMS",
                        "VALOR_CUSTO_COM_ICMS",
                        "VLRCUSTO",
                        "VALOR_CUSTO",
                        "custo_unitario",
                    ],
                )
            ),
        }
    if dataset_key == "itenscomprovantes":
        return {
            "id_produto_shadow": _to_int(_get_any(obj, ["ID_PRODUTOS", "ID_PRODUTO", "id_produto"])),
            "id_grupo_produto_shadow": _to_int(_get_any(obj, ["ID_GRUPOPRODUTOS", "ID_GRUPO_PRODUTO", "id_grupoprodutos"])),
            "id_local_venda_shadow": _to_int(_get_any(obj, ["ID_LOCALVENDAS", "ID_LOCAL_VENDA", "id_localvendas"])),
            "id_funcionario_shadow": _to_int(_get_any(obj, ["ID_FUNCIONARIOS", "ID_FUNCIONARIO", "id_funcionario"])),
            "cfop_shadow": _to_int(_get_any(obj, ["CFOP", "cfop"])),
            "qtd_shadow": _to_numeric(_get_any(obj, ["QTDE", "QTD", "quantidade"])),
            "valor_unitario_shadow": _to_numeric(_get_any(obj, ["VLRUNITARIO", "VALOR_UNITARIO", "valor_unitario"])),
            "total_shadow": _to_numeric(_get_any(obj, ["TOTAL", "VLRTOTALITEM", "VLRTOTAL", "total"])),
            "desconto_shadow": _to_numeric(_get_any(obj, ["VLRDESCONTO", "VALOR_DESCONTO", "desconto"])),
            "custo_unitario_shadow": _to_numeric(
                _get_any(
                    obj,
                    [
                        "VLRCUSTOCOMICMS",
                        "VLR_CUSTO_COM_ICMS",
                        "VALOR_CUSTO_COM_ICMS",
                        "VLRCUSTO",
                        "VALOR_CUSTO",
                        "custo_unitario",
                    ],
                )
            ),
        }
    if dataset_key == "formas_pgto_comprovantes":
        return {
            "valor_shadow": _to_numeric(_get_any(obj, ["VALOR", "VALOR_PAGO", "VALORPAGO", "VLR", "VLR_PAGO", "VLRPAGO", "valor"])),
            "nsu_shadow": _get_any(obj, ["NSU", "nsu"]),
            "autorizacao_shadow": _get_any(obj, ["AUTORIZACAO", "autorizacao"]),
            "bandeira_shadow": _get_any(obj, ["BANDEIRA", "bandeira"]),
            "rede_shadow": _get_any(obj, ["REDE", "rede"]),
            "tef_shadow": _get_any(obj, ["TEF", "tef"]),
        }
    if dataset_key in {"estoque", "estoques"}:
        return {
            "id_produto_shadow": _to_int(_get_any(obj, ["ID_PRODUTOS", "ID_PRODUTO", "id_produto"])),
            "id_local_venda_shadow": _to_int(_get_any(obj, ["ID_LOCALVENDAS", "ID_LOCAL_VENDA", "id_localvendas"])),
            "qtd_atual_shadow": _to_numeric(_get_any(obj, ["QTDEATUAL", "QTD_ATUAL", "qtd_atual"])),
        }
    if dataset_key == "nfe":
        return {
            "status_shadow": _to_int(_get_any(obj, ["STATUS", "status", "STATUSNFE", "STATUS_NFE"])),
            "numero_nfe_shadow": _get_any(obj, ["NRONF", "NUMERO", "NUMERONFE", "NUMERO_NFE", "numero_nfe", "numero"]),
            "serie_shadow": _get_any(obj, ["SERIE", "serie", "SERIENFE", "SERIE_NFE"]),
            "chave_nfe_shadow": _get_any(obj, ["CHAVEACESSO", "CHAVE_ACESSO", "CHAVE", "CHAVENFE", "CHAVE_NFE", "chave_nfe"]),
            "modelo_shadow": _get_any(obj, ["TIPO_DOC", "MODELO", "modelo", "MODELONFE", "MODELO_NFE"]),
            "protocolo_shadow": _get_any(obj, ["PROTOCOLO", "protocolo", "NPROTOCOLO", "NPROTOCOLO_NFE"]),
            "data_emissao_shadow": _parse_ts(_get_any(obj, ["DATA", "TORQMIND_DT_EVENTO", "DATAEMISSAO", "DATA_EMISSAO", "data_emissao"]), tenant_id=None),
            "data_autorizacao_shadow": _parse_ts(_get_any(obj, ["DATAAUTORIZACAO", "DATA_AUTORIZACAO", "data_autorizacao"]), tenant_id=None),
            "data_cancelamento_shadow": _parse_ts(_get_any(obj, ["DATACANCELAMENTO", "DATA_CANCELAMENTO", "data_cancelamento"]), tenant_id=None),
            "data_inutilizacao_shadow": _parse_ts(_get_any(obj, ["DATAINUTILIZACAO", "DATA_INUTILIZACAO", "data_inutilizacao"]), tenant_id=None),
            "valor_nfe_shadow": _to_numeric(_get_any(obj, ["VALOR", "VALORNFE", "VALOR_NFE", "VLRTOTAL", "valor_nfe"])),
        }
    if dataset_key == "controle_troca_pgto":
        return {
            "id_movlctoscancelados_shadow": _to_int(_get_any(obj, ["ID_MOVLCTOSCANCELADOS", "id_movlctoscancelados"])),
            "id_usuario_shadow": _to_int(_get_any(obj, ["USUARIO", "ID_USUARIOS", "ID_USUARIO", "id_usuario", "usuario"])),
            "data_troca_shadow": _parse_ts(_get_any(obj, ["DATA", "TORQMIND_DT_EVENTO", "data"]), tenant_id=None),
        }
    if dataset_key == "movlctoscancelados":
        return {
            "id_planodecontas_shadow": _to_int(_get_any(obj, ["ID_PLANODECONTAS", "id_planodecontas"])),
            "referencia_shadow": _to_int(_get_any(obj, ["REFERENCIA", "referencia", "ID_REFERENCIA", "id_referencia"])),
            "tipo_shadow": _to_int(_get_any(obj, ["TIPO", "tipo"])),
            "valor_shadow": _to_numeric(_get_any(obj, ["VALOR", "VLR", "valor"])),
            "dtaconta_shadow": _parse_ts(_get_any(obj, ["DTACONTA", "TORQMIND_DT_EVENTO", "dtaconta"]), tenant_id=None),
            "id_turno_shadow": _to_int(_get_any(obj, ["ID_TURNOS", "ID_TURNO", "id_turnos", "id_turno"])),
            "ref_operacao_shadow": _to_int(_get_any(obj, ["REF_OPERACAO", "ref_operacao"])),
            "documento_shadow": _get_any(obj, ["DOCUMENTO", "documento"]),
        }
    if dataset_key == "nfe_entrada":
        return {
            "chave_acesso_shadow": _get_any(
                obj, ["CHAVEACESSONFE", "CHAVEACESSO", "CHAVE_ACESSO", "CHAVE", "chave_acesso"]
            ),
            "numero_nota_shadow": _get_any(
                obj, ["NROCOMPROVANTE", "NUMERO", "NRONF", "NUMERONOTA", "numero_nota"]
            ),
            "serie_shadow": _get_any(obj, ["SERIE", "serie"]),
            "cnpj_emitente_shadow": _get_any(obj, ["CNPJEMITENTE", "CNPJ_EMITENTE", "CNPJ", "cnpj_emitente"]),
            "nome_emitente_shadow": _get_any(obj, ["NOMEEMITENTE", "NOME_EMITENTE", "EMITENTE", "nome_emitente"]),
            "dt_entrada_shadow": _parse_ts(
                _get_any(obj, ["DATAENTRADA", "DATA_ENTRADA", "TORQMIND_DT_EVENTO", "DATA", "dt_entrada"]),
                tenant_id=None,
            ),
            "dt_emissao_shadow": _parse_ts(
                _get_any(obj, ["DTAEMISSAO", "DATA", "DATAEMISSAO", "DATA_EMISSAO"]),
                tenant_id=None,
            ),
            "valor_total_shadow": _to_numeric(_get_any(obj, ["VALORTOTAL", "VALOR", "VLRTOTAL", "valor_total"])),
            "situacao_shadow": _to_int(_get_any(obj, ["SITUACAO", "STATUS", "situacao"])),
        }
    if dataset_key == "itens_nfe_entrada":
        qtd = _to_numeric(_get_any(obj, ["QUANTIDADE", "QTDE", "QTD", "qtd"]))
        # Custo de compra preferencial: VLRCUSTO / CUSTO_UNITARIO / VLRUNITARIO
        custo_unit = _to_numeric(
            _get_any(obj, ["CUSTO_UNITARIO", "VLRCUSTO", "custo_unitario", "VLRUNITARIO", "VALOR"])
        )
        custo_total = _to_numeric(_get_any(obj, ["VLRTOTALITEM", "VALORTOTAL", "VLRTOTAL", "custo_total"]))
        if custo_total is None and custo_unit is not None and qtd is not None:
            custo_total = float(custo_unit) * float(qtd)
        tip_comb = _to_int(_get_any(obj, ["TIPOCOMBUSTIVEL", "tipo_combustivel"]))
        nome = _get_any(obj, ["NOMEPRODUTO", "nome_produto"])
        codigo_anp = _get_any(obj, ["CODIGOANP", "codigo_anp"])
        unidade = str(_get_any(obj, ["UNIDADE", "unidade"]) or "").strip().upper()
        eh_comb = bool(tip_comb and tip_comb > 0)
        if not eh_comb and codigo_anp and str(codigo_anp).strip() and unidade == "LT":
            eh_comb = True
        if not eh_comb and nome:
            upper = str(nome).upper().strip()
            eh_comb = (
                upper.startswith("GASOLINA")
                or upper.startswith("ETANOL")
                or upper.startswith("OLEO DIESEL")
                or upper.startswith("DIESEL")
            )
        return {
            "id_produto_shadow": _to_int(_get_any(obj, ["ID_PRODUTOS", "ID_PRODUTO", "id_produto"])),
            "qtd_shadow": qtd,
            "custo_unitario_shadow": custo_unit,
            "custo_total_shadow": custo_total,
            "eh_combustivel_shadow": eh_comb,
        }
    if dataset_key == "preco_bomba_hist":
        return {
            "dt_alteracao_shadow": _parse_ts(
                _get_any(
                    obj,
                    ["DATAALTERACAO", "DTACONTA", "DATA_ALTERACAO", "DATA", "TORQMIND_DT_EVENTO", "dt_alteracao"],
                ),
                tenant_id=None,
            ),
            "preco_venda_shadow": _to_numeric(_get_any(obj, ["PRECO", "PRECOVENDA", "PPL", "preco_venda"])),
            "preco_anterior_shadow": _to_numeric(
                _get_any(obj, ["PRECOANTERIOR", "PRECO_ANTERIOR", "preco_anterior"])
            ),
            "id_bico_shadow": _to_int(_get_any(obj, ["ID_BICOS", "ID_BICO", "id_bico"])),
        }
    return {}


def _batch_columns(dataset_key: str, spec: DatasetSpec) -> List[str]:
    columns = spec.pk_cols + ["id_db_shadow", "id_chave_natural", "dt_evento"]
    if dataset_key == "comprovantes":
        columns.extend(
            [
                "referencia_shadow",
                "id_usuario_shadow",
                "id_turno_shadow",
                "id_cliente_shadow",
                "valor_total_shadow",
                "cancelado_shadow",
                "situacao_shadow",
            ]
        )
    elif dataset_key == "movprodutos":
        columns.extend(
            [
                "id_comprovante_shadow",
                "id_usuario_shadow",
                "id_turno_shadow",
                "id_cliente_shadow",
                "saidas_entradas_shadow",
                "total_venda_shadow",
                "situacao_shadow",
            ]
        )
    elif dataset_key == "itensmovprodutos":
        columns.extend(
            [
                "id_produto_shadow",
                "id_grupo_produto_shadow",
                "id_local_venda_shadow",
                "id_funcionario_shadow",
                "cfop_shadow",
                "qtd_shadow",
                "valor_unitario_shadow",
                "total_shadow",
                "desconto_shadow",
                "custo_unitario_shadow",
            ]
        )
    elif dataset_key == "itenscomprovantes":
        columns.extend(
            [
                "id_produto_shadow",
                "id_grupo_produto_shadow",
                "id_local_venda_shadow",
                "id_funcionario_shadow",
                "cfop_shadow",
                "qtd_shadow",
                "valor_unitario_shadow",
                "total_shadow",
                "desconto_shadow",
                "custo_unitario_shadow",
            ]
        )
    elif dataset_key == "formas_pgto_comprovantes":
        columns.extend(
            [
                "valor_shadow",
                "nsu_shadow",
                "autorizacao_shadow",
                "bandeira_shadow",
                "rede_shadow",
                "tef_shadow",
            ]
        )
    elif dataset_key in {"estoque", "estoques"}:
        columns.extend(
            [
                "id_produto_shadow",
                "id_local_venda_shadow",
                "qtd_atual_shadow",
            ]
        )
    elif dataset_key == "nfe":
        columns.extend(
            [
                "status_shadow",
                "numero_nfe_shadow",
                "serie_shadow",
                "chave_nfe_shadow",
                "modelo_shadow",
                "protocolo_shadow",
                "data_emissao_shadow",
                "data_autorizacao_shadow",
                "data_cancelamento_shadow",
                "data_inutilizacao_shadow",
                "valor_nfe_shadow",
            ]
        )
    elif dataset_key == "controle_troca_pgto":
        columns.extend(
            [
                "id_movlctoscancelados_shadow",
                "id_usuario_shadow",
                "data_troca_shadow",
            ]
        )
    elif dataset_key == "movlctoscancelados":
        columns.extend(
            [
                "id_planodecontas_shadow",
                "referencia_shadow",
                "tipo_shadow",
                "valor_shadow",
                "dtaconta_shadow",
                "id_turno_shadow",
                "ref_operacao_shadow",
                "documento_shadow",
            ]
        )
    elif dataset_key == "nfe_entrada":
        columns.extend(
            [
                "chave_acesso_shadow",
                "numero_nota_shadow",
                "serie_shadow",
                "cnpj_emitente_shadow",
                "nome_emitente_shadow",
                "dt_entrada_shadow",
                "dt_emissao_shadow",
                "valor_total_shadow",
                "situacao_shadow",
            ]
        )
    elif dataset_key == "itens_nfe_entrada":
        columns.extend(
            [
                "id_produto_shadow",
                "qtd_shadow",
                "custo_unitario_shadow",
                "custo_total_shadow",
                "eh_combustivel_shadow",
            ]
        )
    elif dataset_key == "preco_bomba_hist":
        columns.extend(
            [
                "dt_alteracao_shadow",
                "preco_venda_shadow",
                "preco_anterior_shadow",
                "id_bico_shadow",
            ]
        )
    columns.append("payload")
    return columns


def _append_sample(samples: List[Dict[str, Any]], payload: Dict[str, Any]) -> None:
    if len(samples) < 5:
        samples.append(payload)


def _extract_pk_int_alias(obj: Dict[str, Any], keys: List[str]) -> tuple[Optional[int], Optional[str]]:
    seen_values: List[int] = []
    seen_aliases: List[str] = []

    for key in keys:
        if key not in obj or obj[key] is None:
            continue
        parsed = _to_int(obj[key])
        if parsed is None:
            return None, f"Missing/invalid PK fields ({key})"
        seen_aliases.append(key)
        if parsed not in seen_values:
            seen_values.append(parsed)

    if not seen_values:
        return None, "Missing/invalid PK fields"
    if len(seen_values) > 1:
        return None, f"Conflicting PK aliases ({', '.join(seen_aliases)})"
    return seen_values[0], None


class DatasetSpec:
    def __init__(
        self,
        table: str,
        pk_cols: List[str],
        pk_extractors: List[Tuple[str, List[str]]],
    ) -> None:
        self.table = table
        self.pk_cols = pk_cols
        # list of tuples: (dest_col_in_table, [possible keys in payload])
        self.pk_extractors = pk_extractors


DATASETS: Dict[str, DatasetSpec] = {
    # Dimension-like
    "filiais": DatasetSpec(
        table="stg.filiais",
        pk_cols=["id_empresa", "id_filial"],
        pk_extractors=[("id_filial", ["ID_FILIAL", "id_filial"])],
    ),
    "funcionarios": DatasetSpec(
        table="stg.funcionarios",
        pk_cols=["id_empresa", "id_filial", "id_funcionario"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_funcionario", ["ID_FUNCIONARIOS", "ID_FUNCIONARIO", "id_funcionario"]),
        ],
    ),
    "usuarios": DatasetSpec(
        table="stg.usuarios",
        pk_cols=["id_empresa", "id_filial", "id_usuario"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_usuario", ["ID_USUARIOS", "ID_USUARIO", "id_usuario"]),
        ],
    ),
    "entidades": DatasetSpec(
        table="stg.entidades",
        pk_cols=["id_empresa", "id_filial", "id_entidade"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_entidade", ["ID_ENTIDADE", "id_entidade", "ID_CLIENTE"]),
        ],
    ),
    "clientes": DatasetSpec(
        table="stg.entidades",
        pk_cols=["id_empresa", "id_filial", "id_entidade"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_entidade", ["ID_ENTIDADE", "id_entidade", "ID_CLIENTE"]),
        ],
    ),
    "grupoprodutos": DatasetSpec(
        table="stg.grupoprodutos",
        pk_cols=["id_empresa", "id_filial", "id_grupoprodutos"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_grupoprodutos", ["ID_GRUPOPRODUTOS", "id_grupoprodutos"]),
        ],
    ),
    "localvendas": DatasetSpec(
        table="stg.localvendas",
        pk_cols=["id_empresa", "id_filial", "id_localvendas"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_localvendas", ["ID_LOCALVENDAS", "id_localvendas"]),
        ],
    ),
    "produtos": DatasetSpec(
        table="stg.produtos",
        pk_cols=["id_empresa", "id_filial", "id_produto"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_produto", ["ID_PRODUTOS", "ID_PRODUTO", "id_produto"]),
        ],
    ),
    "turnos": DatasetSpec(
        table="stg.turnos",
        pk_cols=["id_empresa", "id_filial", "id_turno"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_turno", ["ID_TURNOS", "ID_TURNO", "id_turno"]),
        ],
    ),

    # Facts raw
    "comprovantes": DatasetSpec(
        table="stg.comprovantes",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_comprovante"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_comprovante", ["ID_COMPROVANTE", "id_comprovante"]),
        ],
    ),
    "movprodutos": DatasetSpec(
        table="stg.movprodutos",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_movprodutos"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_movprodutos", ["ID_MOVPRODUTOS", "id_movprodutos"]),
        ],
    ),
    "movlctos": DatasetSpec(
        table="stg.movlctos",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_movlctos"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_movlctos", ["ID_MOVLCTOS", "ID_MOVLCTO", "id_movlctos", "id_movlcto"]),
        ],
    ),
    # Ajustes TRANSF AJUSTE* (plano bancário sem espelho em MOVBANCOS) — Solvência.
    "movbancos_ajuste_plano": DatasetSpec(
        table="stg.movbancos_ajuste_plano",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_movlctos"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_movlctos", ["ID_MOVLCTOS", "ID_MOVLCTO", "id_movlctos", "id_movlcto"]),
        ],
    ),
    "itensmovprodutos": DatasetSpec(
        table="stg.itensmovprodutos",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_movprodutos", "id_itensmovprodutos"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_movprodutos", ["ID_MOVPRODUTOS", "id_movprodutos"]),
            ("id_itensmovprodutos", ["ID_ITENSMOVPRODUTOS", "id_itensmovprodutos"]),
        ],
    ),
    "itenscomprovantes": DatasetSpec(
        table="stg.itenscomprovantes",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_comprovante", "id_itemcomprovante"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_comprovante", ["ID_COMPROVANTE", "id_comprovante"]),
            (
                "id_itemcomprovante",
                [
                    "ID_ITENSCOMPROVANTE",
                    "ID_ITENS_COMPROVANTE",
                    "ID_ITEMCOMPROVANTE",
                    "id_itemcomprovante",
                ],
            ),
        ],
    ),
    "formas_pgto_comprovantes": DatasetSpec(
        table="stg.formas_pgto_comprovantes",
        pk_cols=["id_empresa", "id_filial", "id_referencia", "tipo_forma"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_referencia", ["ID_REFERENCIA", "id_referencia", "REFERENCIA", "referencia"]),
            ("tipo_forma", ["TIPO_FORMA", "tipo_forma"]),
        ],
    ),
    "estoque": DatasetSpec(
        table="stg.estoque",
        pk_cols=["id_empresa", "id_filial", "id_estoque"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_estoque", ["ID_ESTOQUE", "id_estoque"]),
        ],
    ),
    "estoques": DatasetSpec(
        table="stg.estoque",
        pk_cols=["id_empresa", "id_filial", "id_estoque"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_estoque", ["ID_ESTOQUE", "id_estoque"]),
        ],
    ),

    # Finance
    "contaspagar": DatasetSpec(
        table="stg.contaspagar",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_contaspagar"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_contaspagar", ["ID_CONTASPAGAR", "id_contaspagar"]),
        ],
    ),
    "contasreceber": DatasetSpec(
        table="stg.contasreceber",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_contasreceber"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_contasreceber", ["ID_CONTASRECEBER", "id_contasreceber"]),
        ],
    ),
    "contasreceberbaixa": DatasetSpec(
        table="stg.contasreceberbaixa",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_contasreceberbaixa"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_contasreceberbaixa", ["ID_CONTASRECEBERBAIXA", "id_contasreceberbaixa"]),
        ],
    ),
    "contaspagarbaixa": DatasetSpec(
        table="stg.contaspagarbaixa",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_contaspagarbaixa"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_contaspagarbaixa", ["ID_CONTASPAGARBAIXA", "id_contaspagarbaixa"]),
        ],
    ),
    "financeiro": DatasetSpec(
        table="stg.financeiro",
        pk_cols=["id_empresa", "id_filial", "id_db", "tipo_titulo", "id_titulo"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("tipo_titulo", ["TIPO_TITULO", "tipo_titulo"]),
            ("id_titulo", ["ID_TITULO", "id_titulo"]),
        ],
    ),

    # Financeiro - controle de cheques recebidos + situacoes (motivos)
    "cheques": DatasetSpec(
        table="stg.cheques",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_cheque"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_cheque", ["ID_CHEQUESRECEBIDOS", "id_cheque"]),
        ],
    ),
    "situacoes": DatasetSpec(
        table="stg.situacoes",
        pk_cols=["id_empresa", "id_filial", "id_situacao"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_situacao", ["ID_SITUACOES", "id_situacao"]),
        ],
    ),
    "movcreditoentidades": DatasetSpec(
        table="stg.movcreditoentidades",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_movcredito"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_movcredito", ["ID_MOVCREDITOENTIDADES", "id_movcredito"]),
        ],
    ),
    "credito": DatasetSpec(
        table="stg.credito",
        pk_cols=["id_empresa", "id_filial", "id_credito"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_credito", ["ID_CREDITO", "id_credito"]),
        ],
    ),
    "consolearquivo": DatasetSpec(
        table="stg.consolearquivo",
        pk_cols=["id_empresa", "id_filial", "id_consolearquivo"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_consolearquivo", ["ID_CONSOLEARQUIVO", "id_consolearquivo"]),
        ],
    ),

    # Plano de Contas (chart of accounts)
    "planodecontas": DatasetSpec(
        table="stg.planodecontas",
        pk_cols=["id_empresa", "id_filial", "id_planodecontas"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_planodecontas", ["ID_PLANODECONTAS", "id_planodecontas"]),
        ],
    ),

    # NFE (Nota Fiscal Eletrônica) - fiscal classification
    "nfe": DatasetSpec(
        table="stg.nfe",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_comprovante", "id_nfe"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_comprovante", ["ID_COMPROVANTE", "id_comprovante"]),
            ("id_nfe", ["ID_NFE", "id_nfe"]),
        ],
    ),

    # Compliance ANP — NFe de ENTRADA (compra) + hist. preço bomba
    "nfe_entrada": DatasetSpec(
        table="stg.nfe_entrada",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_nota"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_nota", ["ID_COMPROVANTE", "ID_NOTA", "ID_NOTASFISCAIS", "ID_NFE", "id_nota"]),
        ],
    ),
    "itens_nfe_entrada": DatasetSpec(
        table="stg.itens_nfe_entrada",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_nota", "id_item"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_nota", ["ID_COMPROVANTE", "ID_NOTA", "ID_NOTASFISCAIS", "ID_NFE", "id_nota"]),
            ("id_item", ["ID_ITENSCOMPROVANTE", "ID_ITENSNOTASFISCAIS", "ID_ITEM", "ID_ITENS", "id_item"]),
        ],
    ),
    "preco_bomba_hist": DatasetSpec(
        table="stg.preco_bomba_hist",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_produto", "id_evento"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_produto", ["ID_PRODUTOS", "ID_PRODUTO", "id_produto"]),
            ("id_evento", ["ID_LMCBICOS", "ID_EVENTO", "ID_HISTORICO", "ID_HISTORICOPRECOS", "ID", "id_evento"]),
        ],
    ),
    "descontos_entidades_itens": DatasetSpec(
        table="stg.descontos_entidades_itens",
        pk_cols=["id_empresa", "id_filial", "id_descontoentidadesitens"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            (
                "id_descontoentidadesitens",
                [
                    "ID_DESCONTOENTIDADESITENS",
                    "ID_DESCONTOSENTIDADESITENS",
                    "id_descontoentidadesitens",
                ],
            ),
        ],
    ),

    # Antifraude - troca de forma de pagamento
    "controle_troca_pgto": DatasetSpec(
        table="stg.controle_troca_pgto",
        pk_cols=["id_empresa", "id_filial", "id_db", "id"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id", ["ID", "id"]),
        ],
    ),
    "movlctoscancelados": DatasetSpec(
        table="stg.movlctoscancelados",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_movlctoscancelados"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_movlctoscancelados", ["ID_MOVLCTOSCANCELADOS", "id_movlctoscancelados"]),
        ],
    ),
    "tanques": DatasetSpec(
        table="stg.tanques",
        pk_cols=["id_empresa", "id_filial", "id_tanque"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_tanque", ["ID_TANQUES", "id_tanque"]),
        ],
    ),
    "movtanques": DatasetSpec(
        table="stg.movtanques",
        pk_cols=["id_empresa", "id_filial", "id_movtanque"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_movtanque", ["ID_MOVTANQUES", "id_movtanque"]),
        ],
    ),
    "afericoes": DatasetSpec(
        table="stg.afericoes",
        pk_cols=["id_empresa", "id_filial", "id_afericao"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_afericao", ["ID_AFERICAO", "id_afericao"]),
        ],
    ),
    "bicos": DatasetSpec(
        table="stg.bicos",
        pk_cols=["id_empresa", "id_filial", "id_bico"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_bico", ["ID_BICOS", "id_bico"]),
        ],
    ),
    # Solvencia DRE: prazo de repasse de cartoes (join ID_CARTAO -> ID_CONVENIOS)
    "convenios": DatasetSpec(
        table="stg.convenios",
        pk_cols=["id_empresa", "id_filial", "id_convenios"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_convenios", ["ID_CONVENIOS", "id_convenios"]),
        ],
    ),
    "movbancos": DatasetSpec(
        table="stg.movbancos",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_movbancos"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_movbancos", ["ID_MOVBANCOS", "id_movbancos"]),
        ],
    ),
    "contasbancaria": DatasetSpec(
        table="stg.contasbancaria",
        pk_cols=["id_empresa", "id_filial", "id_contasbancarias"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_contasbancarias", ["ID_CONTASBANCARIAS", "id_contasbancarias"]),
        ],
    ),
    "bancospadrao": DatasetSpec(
        table="stg.bancospadrao",
        pk_cols=["id_empresa", "id_filial", "id_bancospadrao"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_bancospadrao", ["ID_BANCOSPADRAO", "id_bancospadrao"]),
        ],
    ),
    "saldoclientes": DatasetSpec(
        table="stg.saldoclientes",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_saldoclientes"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_saldoclientes", ["ID_SALDOCLIENTES", "id_saldoclientes"]),
        ],
    ),
}


def _resolve_id_empresa(x_ingest_key: Optional[str], x_empresa_id: Optional[str]) -> int:
    """Resolve tenant id from X-Ingest-Key, fallback to X-Empresa-Id (dev only)."""

    if x_ingest_key:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(
                "SELECT id_empresa FROM app.tenants WHERE ingest_key = %s AND is_active = true",
                (x_ingest_key,),
            ).fetchone()
            if not row:
                raise HTTPException(status_code=401, detail="Invalid X-Ingest-Key")
            return int(row["id_empresa"])

    if settings.ingest_require_key:
        raise HTTPException(status_code=401, detail="Missing X-Ingest-Key")

    # Dev fallback
    if x_empresa_id:
        try:
            return int(x_empresa_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid X-Empresa-Id")

    return 1


def _load_tenant_ingest_policy(id_empresa: int) -> Dict[str, Any]:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(
            """
            SELECT
              id_empresa,
              sales_history_days,
              default_product_scope_days
            FROM app.tenants
            WHERE id_empresa = %s
            """,
            (id_empresa,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail={"error": "tenant_not_found", "message": "Empresa não encontrada."})
    payload = dict(row)
    payload["ref_date"] = business_today(id_empresa)
    return payload


def _sales_retention_cutoff(ref_date: date, days: int) -> date:
    window_days = max(int(days or 365), 1)
    return ref_date - timedelta(days=window_days - 1)


def _configured_retention_override_datasets() -> List[str]:
    datasets = [
        part.strip().lower()
        for part in str(settings.ingest_retention_override_datasets or "").split(",")
        if part.strip()
    ]
    return sorted(set(datasets))


def _retention_policy_response(dataset_key: str, tenant_policy: Dict[str, Any]) -> Dict[str, Any]:
    enforced = dataset_key in SALES_RETENTION_DATASETS
    days = None
    default_cutoff = None
    effective_cutoff = None
    override_min_date = settings.ingest_retention_override_min_date
    override_datasets = _configured_retention_override_datasets()
    override_active = False
    if enforced:
        days = int(tenant_policy.get("sales_history_days") or 365)
        default_cutoff = _sales_retention_cutoff(tenant_policy["ref_date"], days)
        effective_cutoff = default_cutoff
        if override_min_date and dataset_key in override_datasets and override_min_date < default_cutoff:
            effective_cutoff = override_min_date
            override_active = True
    return {
        "name": "sales_history_days" if enforced else "none",
        "enforced": enforced,
        "days": days,
        "cutoff": effective_cutoff.isoformat() if effective_cutoff else None,
        "default_cutoff": default_cutoff.isoformat() if default_cutoff else None,
        "cutoff_source": "override_min_date" if override_active else ("sales_history_days" if enforced else "none"),
        "business_date_field": "dt_evento",
        "datasets": sorted(SALES_RETENTION_DATASETS) if enforced else [],
        "override": {
            "configured": override_min_date is not None,
            "active": override_active,
            "min_date": override_min_date.isoformat() if override_min_date else None,
            "datasets": override_datasets,
        },
    }


async def _stream_ndjson_objects(request: Request, is_gzip: bool) -> AsyncIterator[Dict[str, Any]]:
    buffer = b""
    line_no = 0
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS) if is_gzip else None

    def flush_lines(chunk_buffer: bytes) -> Tuple[List[bytes], bytes]:
        lines = chunk_buffer.splitlines(keepends=True)
        if lines and not lines[-1].endswith((b"\n", b"\r")):
            return lines[:-1], lines[-1]
        return lines, b""

    async for chunk in request.stream():
        if not chunk:
            continue
        if decompressor is not None:
            try:
                chunk = decompressor.decompress(chunk)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"Invalid gzip body: {exc}")
        if not chunk:
            continue
        buffer += chunk
        lines, buffer = flush_lines(buffer)
        for raw_line in lines:
            line = raw_line.strip()
            if not line:
                continue
            line_no += 1
            try:
                obj = json.loads(line)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"Invalid NDJSON at line {line_no}: {exc}")
            if not isinstance(obj, dict):
                raise HTTPException(status_code=400, detail=f"Invalid NDJSON at line {line_no}: line is not an object")
            yield obj

    if decompressor is not None:
        try:
            buffer += decompressor.flush()
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid gzip body: {exc}")

    if buffer.strip():
        line_no += 1
        try:
            obj = json.loads(buffer)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid NDJSON at line {line_no}: {exc}")
        if not isinstance(obj, dict):
            raise HTTPException(status_code=400, detail=f"Invalid NDJSON at line {line_no}: line is not an object")
        yield obj


def _bulk_upsert_with_stats(
    conn,
    dataset_key: str,
    table: str,
    pk_cols: List[str],
    rows: List[Tuple[Any, ...]],
) -> Tuple[int, int, int]:
    """Bulk upsert with no-op suppression.

    Rows whose non-PK columns (incl. payload) are unchanged are NOT rewritten and
    do not bump ingested_at/received_at — critical to cut WAL/CDC/CH write amplification
    from agent full_refresh cycles.

    Returns (inserted, updated, unchanged).
    """
    if not rows:
        return 0, 0, 0

    cols = _batch_columns(dataset_key, DATASETS[dataset_key])
    schema_name, table_name = table.split(".", 1)
    temp_name = f"tmp_ingest_{table_name}_{uuid4().hex[:8]}"
    cols_sql = sql.SQL(", ").join(sql.Identifier(col) for col in cols)
    conflict_sql = sql.SQL(", ").join(sql.Identifier(col) for col in pk_cols)
    update_cols = [col for col in cols if col not in pk_cols]
    if not update_cols:
        # Defensive: should never happen (payload always present).
        update_cols = ["payload"]
    update_sql = sql.SQL(", ").join(
        [
            *[
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                for col in update_cols
            ],
            sql.SQL("ingested_at = now()"),
            sql.SQL("received_at = now()"),
        ]
    )
    # Only rewrite when something material changed (payload and/or shadow cols).
    change_where = sql.SQL(" OR ").join(
        [
            sql.SQL("target.{} IS DISTINCT FROM EXCLUDED.{}").format(
                sql.Identifier(col), sql.Identifier(col)
            )
            for col in update_cols
        ]
    )

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP").format(
                sql.Identifier(temp_name),
                sql.Identifier(schema_name, table_name),
            )
        )
        with cur.copy(sql.SQL("COPY {} ({}) FROM STDIN").format(sql.Identifier(temp_name), cols_sql)) as copy:
            for row in rows:
                copy.write_row(row)
        # WHEN WHERE is false, RETURNING omits the row → unchanged = len(rows) - len(flags).
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {} AS target ({})
                SELECT {}
                FROM {}
                ON CONFLICT ({})
                DO UPDATE SET {}
                WHERE {}
                RETURNING (xmax = 0) AS inserted_flag
                """
            ).format(
                sql.Identifier(schema_name, table_name),
                cols_sql,
                cols_sql,
                sql.Identifier(temp_name),
                conflict_sql,
                update_sql,
                change_where,
            )
        )
        flags = cur.fetchall()
    inserted = sum(1 for f in flags if f["inserted_flag"])
    updated = len(flags) - inserted
    unchanged = len(rows) - len(flags)
    return inserted, updated, unchanged


def _dedupe_rows_by_pk(pk_cols: List[str], rows: List[Tuple[Any, ...]]) -> Tuple[List[Tuple[Any, ...]], int]:
    """Deduplicate rows in-memory by PK tuple, preserving the last occurrence.

    This avoids Postgres error:
    'ON CONFLICT DO UPDATE command cannot affect row a second time'
    when a single VALUES batch contains duplicate PKs.
    """
    if not rows:
        return rows, 0

    pk_size = len(pk_cols)
    by_pk: Dict[Tuple[Any, ...], Tuple[Any, ...]] = {}
    for row in rows:
        by_pk[tuple(row[:pk_size])] = row
    deduped = list(by_pk.values())
    duplicates = len(rows) - len(deduped)
    return deduped, duplicates


@router.get("/health")
def ingest_health(
    x_ingest_key: Optional[str] = Header(None, alias="X-Ingest-Key"),
    x_empresa_id: Optional[str] = Header(None, alias="X-Empresa-Id"),
    stats: bool = Query(
        False,
        description="If true, run per-dataset COUNT/MAX (slow; can 504 behind nginx). Default is auth-only.",
    ),
):
    """Agent connectivity check — must stay fast.

    Default: resolve tenant from X-Ingest-Key and return ok.
    Optional ``?stats=true`` keeps the legacy heavy STG scan for ops/debug only.
    """
    id_empresa = _resolve_id_empresa(x_ingest_key=x_ingest_key, x_empresa_id=x_empresa_id)
    if not stats:
        return {
            "ok": True,
            "id_empresa": id_empresa,
            "mode": "auth",
            "datasets": [],
        }

    out: List[Dict[str, Any]] = []
    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        for dataset, spec in sorted(DATASETS.items()):
            row = conn.execute(
                f"""
                SELECT
                  COUNT(*)::bigint AS rows_total,
                  MAX(ingested_at) AS max_ingested_at,
                  MAX(received_at) AS max_received_at,
                  MAX(dt_evento) AS max_dt_evento
                FROM {spec.table}
                WHERE id_empresa = %s
                """,
                (id_empresa,),
            ).fetchone()
            out.append(
                {
                    "dataset": dataset,
                    "table": spec.table,
                    "rows_total": int(row["rows_total"] or 0),
                    "max_ingested_at": row["max_ingested_at"],
                    "max_received_at": row["max_received_at"],
                    "max_dt_evento": row["max_dt_evento"],
                }
            )
    return {"ok": True, "id_empresa": id_empresa, "mode": "stats", "datasets": out}


@router.post("/{dataset}")
async def ingest_dataset(
    dataset: str,
    request: Request,
    run_etl: bool = Query(False, description="Run ETL right after ingest"),
    refresh_mart: bool = Query(True, description="If run_etl, also refresh materialized views"),
    x_ingest_key: Optional[str] = Header(None, alias="X-Ingest-Key"),
    x_empresa_id: Optional[str] = Header(None, alias="X-Empresa-Id"),
):
    dataset_key = dataset.strip().lower()
    if dataset_key not in DATASETS:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Unknown dataset",
                "dataset": dataset,
                "allowed": sorted(DATASETS.keys()),
            },
        )

    id_empresa = _resolve_id_empresa(x_ingest_key=x_ingest_key, x_empresa_id=x_empresa_id)
    spec = DATASETS[dataset_key]
    tenant_policy = await asyncio.to_thread(_load_tenant_ingest_policy, id_empresa)
    retention_policy = _retention_policy_response(dataset_key, tenant_policy)
    retention_cutoff = (
        date.fromisoformat(retention_policy["cutoff"])
        if retention_policy.get("cutoff")
        else None
    )
    retention_override = retention_policy.get("override") or {}
    if retention_override.get("active"):
        logger.warning(
            "Ingest retention override active tenant=%s dataset=%s min_date=%s default_cutoff=%s effective_cutoff=%s",
            id_empresa,
            dataset_key,
            retention_override.get("min_date"),
            retention_policy.get("default_cutoff"),
            retention_policy.get("cutoff"),
        )

    is_gzip = (request.headers.get("content-encoding") or "").lower() == "gzip"
    batch_size = max(int(settings.ingest_batch_size or 5000), 1)
    batch_values: List[Tuple[Any, ...]] = []
    rejected_invalid_count = 0
    rejected_by_retention_count = 0
    rejected_samples: List[Dict[str, Any]] = []
    cancelled_rows: List[Dict[str, Any]] = []
    voided_nfe_rows: List[Dict[str, Any]] = []
    preco_fixo_item_rows: List[Dict[str, Any]] = []
    inserted = 0
    updated = 0
    unchanged = 0
    duplicates_in_batch = 0
    inserted_or_updated = 0

    def flush_batch() -> None:
        nonlocal batch_values, inserted, updated, unchanged, duplicates_in_batch, inserted_or_updated
        if not batch_values:
            return
        batch_values, batch_duplicates = _dedupe_rows_by_pk(spec.pk_cols, batch_values)
        duplicates_in_batch += batch_duplicates
        with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
            with conn.transaction():
                batch_inserted, batch_updated, batch_unchanged = _bulk_upsert_with_stats(
                    conn, dataset_key, spec.table, spec.pk_cols, batch_values
                )
            conn.commit()
        inserted += batch_inserted
        updated += batch_updated
        unchanged += batch_unchanged
        inserted_or_updated += batch_inserted + batch_updated
        batch_values = []

    async for obj in _stream_ndjson_objects(request, is_gzip=is_gzip):
        obj = _strip_null_chars(obj)
        pk: Dict[str, Any] = {"id_empresa": id_empresa}

        ok = True
        pk_error_reason = "Missing/invalid PK fields"
        for dest_col, keys in spec.pk_extractors:
            iv, reason = _extract_pk_int_alias(obj, keys)
            if iv is None:
                ok = False
                pk_error_reason = reason or pk_error_reason
                break
            pk[dest_col] = iv

        if not ok:
            rejected_invalid_count += 1
            _append_sample(rejected_samples, {"row": obj, "reason": pk_error_reason})
            continue

        if "id_filial" in spec.pk_cols and "id_filial" not in pk:
            rejected_invalid_count += 1
            _append_sample(rejected_samples, {"row": obj, "reason": "Missing id_filial"})
            continue

        dt_evento = _infer_dt_evento(obj, tenant_id=id_empresa)
        dt_evento_business_date = business_date_for_datetime(dt_evento, tenant_id=id_empresa) if dt_evento is not None else None
        if retention_cutoff is not None and dt_evento_business_date is not None and dt_evento_business_date < retention_cutoff:
            rejected_by_retention_count += 1
            _append_sample(
                rejected_samples,
                {
                    "row": obj,
                    "reason": "Rejected by sales_history_days retention window",
                    "dt_evento": dt_evento.isoformat(),
                    "business_date": dt_evento_business_date.isoformat(),
                    "cutoff": retention_cutoff.isoformat(),
                },
            )
            continue

        if dataset_key == "comprovantes" and raw_comprovante_is_cancelled(obj):
            cancelled_rows.append(obj)

        if dataset_key == "nfe" and raw_nfe_is_voided(obj):
            voided_nfe_rows.append(obj)

        if dataset_key == "itenscomprovantes" and len(preco_fixo_item_rows) < 200:
            preco_fixo_item_rows.append(obj)

        id_db_shadow = _infer_id_db_shadow(obj)
        natural_key = _infer_natural_key(obj, pk)
        payload_json = json.dumps(obj, ensure_ascii=False)
        shadow_values = _shadow_values_for_dataset(dataset_key, obj)

        tuple_values = [pk.get(col) for col in spec.pk_cols]
        tuple_values.append(id_db_shadow)
        tuple_values.append(natural_key)
        tuple_values.append(dt_evento)
        for col in _batch_columns(dataset_key, spec)[len(spec.pk_cols) + 3 : -1]:
            tuple_values.append(shadow_values.get(col))
        tuple_values.append(payload_json)
        batch_values.append(tuple(tuple_values))

        if len(batch_values) >= batch_size:
            await asyncio.to_thread(flush_batch)

    await asyncio.to_thread(flush_batch)

    logger.info(
        "Ingest summary tenant=%s dataset=%s inserted=%s updated=%s unchanged=%s duplicates_in_batch=%s rejected_invalid=%s rejected_by_retention=%s cutoff=%s cutoff_source=%s override_active=%s",
        id_empresa,
        dataset_key,
        inserted,
        updated,
        unchanged,
        duplicates_in_batch,
        rejected_invalid_count,
        rejected_by_retention_count,
        retention_policy.get("cutoff"),
        retention_policy.get("cutoff_source"),
        bool(retention_override.get("active")),
    )

    if not inserted_or_updated:
        return {
            "ok": True,
            "dataset": dataset_key,
            "id_empresa": id_empresa,
            "inserted_or_updated": 0,
            "inserted": inserted,
            "updated": updated,
            "unchanged": unchanged,
            "duplicates_in_batch": duplicates_in_batch,
            "rejected": rejected_invalid_count + rejected_by_retention_count,
            "rejected_invalid": rejected_invalid_count,
            "rejected_by_retention": rejected_by_retention_count,
            "retention_cutoff": retention_policy.get("cutoff"),
            "retention_policy": retention_policy,
            "sample_rejections": rejected_samples,
            "details": rejected_samples,
        }

    # Optional: send telegram notifications when there are cancelled comprovantes
    if dataset_key == "comprovantes" and cancelled_rows:
        try:
            await notify_cancelled_comprovantes(id_empresa=id_empresa, raw_rows=cancelled_rows)
        except Exception:
            # Never fail ingestion due to notification issues.
            pass

    # Optional: send telegram notifications when there are voided NFEs (status=5)
    if dataset_key == "nfe" and voided_nfe_rows:
        try:
            await notify_voided_nfes(id_empresa=id_empresa, raw_rows=voided_nfe_rows)
        except Exception:
            pass

    # Preço fixo × bomba: checa na ingestão de itens (lote EOD cobre miss)
    if dataset_key == "itenscomprovantes" and preco_fixo_item_rows:
        try:
            notify_preco_fixo_from_itens(id_empresa, preco_fixo_item_rows)
        except Exception:
            pass

    etl_result = None
    if run_etl:
        try:
            summary = await asyncio.to_thread(
                run_incremental_cycle,
                [id_empresa],
                ref_date=business_today(id_empresa),
                refresh_mart=refresh_mart,
                force_full=False,
                fail_fast=True,
                track=TRACK_OPERATIONAL,
                skip_busy_tenants=True,
                db_role="MASTER",
                db_tenant_scope=id_empresa,
                tenant_rows=[{"id_empresa": id_empresa}],
                acquire_lock=True,
            )
            item = (summary.get("items") or [None])[0] or {}
            if item.get("skipped"):
                etl_result = {
                    "ok": True,
                    "track": TRACK_OPERATIONAL,
                    "skipped": True,
                    "reason": item.get("reason"),
                    "message": item.get("message"),
                }
            else:
                etl_result = item.get("result")
            if item.get("ok") is False and not etl_result:
                etl_result = {
                    "ok": False,
                    "error": "etl_failed",
                    "message": str(item.get("error") or "Falha ao executar ETL após ingestão."),
                }
        except EtlCycleBusyError as exc:
            etl_result = {
                "ok": False,
                "error": "etl_busy",
                "message": str(exc),
            }

    return {
        "ok": True,
        "dataset": dataset_key,
        "id_empresa": id_empresa,
        "inserted_or_updated": inserted_or_updated,
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "duplicates_in_batch": duplicates_in_batch,
        "rejected": rejected_invalid_count + rejected_by_retention_count,
        "rejected_invalid": rejected_invalid_count,
        "rejected_by_retention": rejected_by_retention_count,
        "retention_cutoff": retention_policy.get("cutoff"),
        "retention_policy": retention_policy,
        "etl": etl_result,
        "sample_rejections": rejected_samples,
    }
