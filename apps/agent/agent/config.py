from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import copy
import os
from typing import Any, Dict, Optional

import yaml

from agent.secrets import SecretStoreError, load_encrypted_json_file, save_encrypted_json_file


COMMERCIAL_WINDOW_DAYS = 365
# Solvencia/DRE: historico desde Jan/2025 (~560d em Jul/2026). Mantem throttle
# (batch_delay_seconds) para nao estourar CPU no SQL Server do cliente.
SOLVENCIA_BOOTSTRAP_DAYS = 560
DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS = 240
EVENT_DATE_ALIAS = "TORQMIND_DT_EVENTO"
WATERMARK_ALIAS = "TORQMIND_WATERMARK"
LEGACY_SENTINEL_DATETIME_SQL = "1900-01-01T00:00:00"
FORMAS_PGTO_COMPROVANTES_REFERENCE_EXPR = (
    "CASE "
    "WHEN NULLIF(LTRIM(RTRIM(CAST(f.ID_REFERENCIA AS varchar(64)))), '') IS NOT NULL "
    " AND LTRIM(RTRIM(CAST(f.ID_REFERENCIA AS varchar(64)))) NOT LIKE '%[^0-9]%' "
    "THEN CAST(LTRIM(RTRIM(CAST(f.ID_REFERENCIA AS varchar(64)))) AS int) "
    "ELSE NULL END"
)


DEFAULT_DATASETS: Dict[str, Dict[str, Any]] = {
    "filiais": {
        "table": "dbo.FILIAIS",
        "watermark_column": "ID_FILIAL",
        "watermark_order_by": "ID_FILIAL",
        "full_refresh": True,
        "enabled": False,
    },
    "funcionarios": {
        "table": "dbo.FUNCIONARIOS",
        "watermark_column": "ID_FUNCIONARIOS",
        "watermark_order_by": "ID_FUNCIONARIOS, ID_FILIAL",
        "full_refresh": True,
        "enabled": False,
    },
    "usuarios": {
        "table": "dbo.USUARIOS",
        "watermark_column": "ID_USUARIOS",
        "watermark_order_by": "ID_USUARIOS, ID_FILIAL",
        "full_refresh": True,
        "enabled": True,
    },
    "entidades": {
        "table": "dbo.ENTIDADES",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": WATERMARK_ALIAS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT e.*, "
            "COALESCE(CAST(e.ULTALTERACAO AS datetime2), CAST(e.DTACADASTRO AS datetime2)) AS TORQMIND_WATERMARK "
            "FROM dbo.ENTIDADES e"
        ),
        "enabled": False,
    },
    "clientes": {
        "table": "dbo.ENTIDADES",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": WATERMARK_ALIAS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT e.*, "
            "COALESCE(CAST(e.ULTALTERACAO AS datetime2), CAST(e.DTACADASTRO AS datetime2)) AS TORQMIND_WATERMARK "
            "FROM dbo.ENTIDADES e"
        ),
        "enabled": False,
    },
    "grupoprodutos": {
        "table": "dbo.GRUPOPRODUTOS",
        "watermark_column": "ID_GRUPOPRODUTOS",
        "watermark_order_by": "ID_GRUPOPRODUTOS, ID_FILIAL",
        "full_refresh": True,
        "enabled": False,
    },
    "planodecontas": {
        "table": "dbo.PLANODECONTAS",
        "watermark_column": "ID_PLANODECONTAS",
        "watermark_order_by": "ID_PLANODECONTAS, ID_FILIAL",
        "full_refresh": True,
        "enabled": True,
    },
    "localvendas": {
        "table": "dbo.LOCALVENDAS",
        "watermark_column": "ID_LOCALVENDAS",
        "watermark_order_by": "ID_LOCALVENDAS, ID_FILIAL",
        "full_refresh": True,
        "enabled": False,
    },
    "produtos": {
        "table": "dbo.PRODUTOS",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": WATERMARK_ALIAS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT p.*, "
            "COALESCE(CAST(p.ULTALTERACAO AS datetime2), CAST(p.DATACADASTRO AS datetime2)) AS TORQMIND_WATERMARK "
            "FROM dbo.PRODUTOS p"
        ),
        # Necessario para o custo (CUSTOMEDIO) e grupo (ID_GRUPOPRODUTOS) usados no
        # estoque valorizado da aba Solvencia.
        "enabled": True,
    },
    "turnos": {
        "table": "dbo.TURNOS",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_order_by": f"{WATERMARK_ALIAS}, ID_FILIAL, ID_TURNOS",
        "allow_zero_inserted_batches": True,
        "contract_name": "turnos_pk",
        "required_fields": ["ID_TURNOS", "ID_FILIAL"],
        "unique_key_fields": ["ID_TURNOS", "ID_FILIAL"],
        "preflight_tables": {
            "dbo.TURNOS": [
                "ID_TURNOS",
                "ID_FILIAL",
                "DATA",
                "DATATURNO",
                "DATAFECHAMENTO",
                "ENCERRANTEFECHAMENTO",
            ]
        },
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT t.*, "
            "CAST(t.DATA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) "
            "   FROM (VALUES "
            "       (CAST(t.DATA AS datetime2)), "
            "       (CAST(t.DATATURNO AS datetime2)), "
            "       (CAST(t.DATAFECHAMENTO AS datetime2))"
            "   ) AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.TURNOS t"
        ),
        "enabled": True,
    },
    "comprovantes": {
        "table": "dbo.COMPROVANTES",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_order_by": f"{WATERMARK_ALIAS}, ID_COMPROVANTE, ID_FILIAL, ID_DB",
        "cursor_pk_columns": ["ID_COMPROVANTE", "ID_FILIAL", "ID_DB"],
        "contract_name": "comprovantes_pk_required_fields",
        "required_fields": ["ID_COMPROVANTE", "ID_FILIAL", "ID_DB", "SITUACAO"],
        "unique_key_fields": ["ID_COMPROVANTE", "ID_FILIAL", "ID_DB"],
        "preflight_tables": {
            "dbo.COMPROVANTES": [
                "ID_COMPROVANTE",
                "ID_FILIAL",
                "ID_DB",
                "DATA",
                "DATAREPL",
                "SITUACAO",
            ]
        },
        "retention_days": COMMERCIAL_WINDOW_DAYS,
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT c.*, "
            "CAST(c.DATA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) "
            "   FROM (VALUES "
            "       (CAST(c.DATA AS datetime2)), "
            f"       (NULLIF(CAST(c.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            "   ) AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.COMPROVANTES c"
        ),
        "enabled": True,
    },
    "itenscomprovantes": {
        "table": "dbo.ITENSCOMPROVANTE",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_order_by": f"{WATERMARK_ALIAS}, ID_FILIAL, ID_ITENSCOMPROVANTE, ID_DB",
        "cursor_pk_columns": ["ID_FILIAL", "ID_ITENSCOMPROVANTE", "ID_DB"],
        "contract_name": "itenscomprovantes_pk_parent",
        "required_fields": ["ID_ITENSCOMPROVANTE", "ID_ITEMCOMPROVANTE", "ID_FILIAL", "ID_DB", "ID_COMPROVANTE"],
        "unique_key_fields": ["ID_FILIAL", "ID_ITENSCOMPROVANTE", "ID_DB"],
        # Keep the raw SQL Server key and add an explicit canonical alias expected by TorqMind.
        "row_aliases": {"ID_ITEMCOMPROVANTE": "ID_ITENSCOMPROVANTE"},
        "preflight_tables": {
            "dbo.ITENSCOMPROVANTE": [
                "ID_ITENSCOMPROVANTE",
                "ID_FILIAL",
                "ID_DB",
                "ID_COMPROVANTE",
                "DATAREPL",
            ],
            "dbo.COMPROVANTES": [
                "ID_COMPROVANTE",
                "ID_FILIAL",
                "ID_DB",
                "DATA",
            ],
        },
        "retention_days": COMMERCIAL_WINDOW_DAYS,
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT i.*, "
            "CAST(c.DATA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) "
            "   FROM (VALUES "
            "       (CAST(c.DATA AS datetime2)), "
            f"       (NULLIF(CAST(i.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            "   ) AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.ITENSCOMPROVANTE i "
            "JOIN dbo.COMPROVANTES c "
            "  ON c.ID_COMPROVANTE = i.ID_COMPROVANTE "
            " AND c.ID_FILIAL = i.ID_FILIAL "
            " AND c.ID_DB = i.ID_DB"
        ),
        "enabled": True,
    },
    "movprodutos": {
        "table": "dbo.MOVPRODUTOS",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_order_by": f"{WATERMARK_ALIAS}, ID_MOVPRODUTOS, ID_FILIAL, ID_DB",
        "cursor_pk_columns": ["ID_MOVPRODUTOS", "ID_FILIAL", "ID_DB"],
        # NAO e fonte canonica de venda (use comprovantes). Necessario para
        # reconstruir estoque loja as-of na Solvencia (movimentacao + custo NF).
        "retention_days": SOLVENCIA_BOOTSTRAP_DAYS,
        "bootstrap_days": SOLVENCIA_BOOTSTRAP_DAYS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT m.*, "
            "CAST(m.DATA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) "
            "   FROM (VALUES "
            "       (CAST(m.DATA AS datetime2)), "
            f"       (NULLIF(CAST(m.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            "   ) AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.MOVPRODUTOS m"
        ),
        "enabled": True,
        "deprecated": False,
        "deprecation_notice": (
            "Not canonical for sales (use comprovantes). Enabled for Solvencia "
            "stock reconstruction (movimentacao de estoque)."
        ),
    },
    "itensmovprodutos": {
        "table": "dbo.ITENSMOVPRODUTOS",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_order_by": f"{WATERMARK_ALIAS}, ID_ITENSMOVPRODUTOS, ID_FILIAL, ID_DB",
        "cursor_pk_columns": ["ID_ITENSMOVPRODUTOS", "ID_FILIAL", "ID_DB"],
        # Custo de reposicao = VLRCUSTOCOMICMS da ultima NF de entrada (CFOP 1.xxx/2.xxx).
        "retention_days": SOLVENCIA_BOOTSTRAP_DAYS,
        "bootstrap_days": SOLVENCIA_BOOTSTRAP_DAYS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT i.*, "
            "CAST(m.DATA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) "
            "   FROM (VALUES "
            "       (CAST(m.DATA AS datetime2)), "
            f"       (NULLIF(CAST(i.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            "   ) AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.ITENSMOVPRODUTOS i "
            "JOIN dbo.MOVPRODUTOS m "
            "  ON m.ID_MOVPRODUTOS = i.ID_MOVPRODUTOS "
            " AND m.ID_FILIAL = i.ID_FILIAL "
            " AND m.ID_DB = i.ID_DB"
        ),
        "enabled": True,
        "deprecated": False,
        "deprecation_notice": (
            "Not canonical for sales items (use itenscomprovantes). Enabled for "
            "Solvencia stock cost (VLRCUSTOCOMICMS)."
        ),
    },
    "formas_pgto_comprovantes": {
        "table": "dbo.FORMAS_PGTO_COMPROVANTES",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_order_by": f"{WATERMARK_ALIAS}, ID_FILIAL, ID_FORMAS_PGTO_COMPROVANTES, ID_DB",
        "cursor_pk_columns": ["ID_FILIAL", "ID_FORMAS_PGTO_COMPROVANTES", "ID_DB"],
        "contract_name": "formas_pgto_comprovantes_pk",
        "required_fields": ["ID_FORMAS_PGTO_COMPROVANTES", "ID_FILIAL", "ID_DB", "ID_REFERENCIA"],
        "unique_key_fields": ["ID_FILIAL", "ID_FORMAS_PGTO_COMPROVANTES", "ID_DB"],
        "preflight_tables": {
            "dbo.FORMAS_PGTO_COMPROVANTES": [
                "ID_FORMAS_PGTO_COMPROVANTES",
                "ID_FILIAL",
                "ID_DB",
                "ID_REFERENCIA",
                "DATAREPL",
            ],
            "dbo.COMPROVANTES": [
                "ID_COMPROVANTE",
                "ID_FILIAL",
                "ID_DB",
                "REFERENCIA",
                "DATA",
            ],
        },
        "retention_days": COMMERCIAL_WINDOW_DAYS,
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT f.*, "
            "CAST(c.DATA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) "
            "   FROM (VALUES "
            "       (CAST(c.DATA AS datetime2)), "
            f"       (NULLIF(CAST(f.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            "   ) AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.FORMAS_PGTO_COMPROVANTES f "
            "JOIN dbo.COMPROVANTES c "
            f"  ON c.REFERENCIA = {FORMAS_PGTO_COMPROVANTES_REFERENCE_EXPR} "
            " AND c.ID_FILIAL = f.ID_FILIAL "
            " AND c.ID_DB = f.ID_DB"
        ),
        "enabled": True,
    },
    "contaspagar": {
        "table": "dbo.CONTASPAGAR",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        # DATAREPL do CP muitas vezes não acompanha baixa/vencimento (mesmo padrão
        # de CONTASRECEBER). Sem revisit, DRE perde títulos do mês (VR01 jun/2026).
        "revisit_open_clause": (
            "(DTAPGTO IS NULL AND CAST(DTACONTA AS date) >= CAST(DATEADD(day,-120,GETDATE()) AS date)) "
            "OR (DTAPGTO IS NOT NULL AND CAST(DTAPGTO AS date) >= CAST(DATEADD(day,-120,GETDATE()) AS date)) "
            "OR (CAST(DTAVCTO AS date) >= CAST(DATEADD(day,-120,GETDATE()) AS date))"
        ),
        "query": (
            "SELECT c.*, "
            "CAST(c.DTACONTA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) FROM (VALUES "
            "(CAST(c.DTACONTA AS datetime2)), "
            f"(NULLIF(CAST(c.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2))), "
            "(CAST(c.DTAPGTO AS datetime2)), "
            "(CAST(c.DTAVCTO AS datetime2))"
            ") AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.CONTASPAGAR c"
        ),
        "enabled": True,
    },
    "contasreceber": {
        "table": "dbo.CONTASRECEBER",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        # revisit_open_clause roda no WHERE externo (SELECT * FROM (base) AS src),
        # onde o alias `c` da query base nao existe. As colunas vem de `c.*`, entao
        # devem ser referenciadas SEM prefixo (senao "multi-part identifier could not be bound").
        #
        # IMPORTANTE (pagamento direto): quando um titulo e PAGO direto na
        # CONTASRECEBER (DTAPGTO/VLRPAGO preenchidos) o ERP NAO atualiza DATAREPL
        # (fica no sentinela 1900-01-01) e o watermark composto pode estar
        # envenenado por linha suja (data futura), entao o incremental nao captura
        # a baixa. A rede de seguranca precisa reler tanto os titulos AINDA ABERTOS
        # quanto os RECEM-PAGOS (ultimos 120 dias por DTAPGTO), senao o titulo pago
        # congela como aberto no STG -> DW -> mart de inadimplencia.
        "revisit_open_clause": (
            "(DTAPGTO IS NULL AND CAST(DTACONTA AS date) >= CAST(DATEADD(day,-90,GETDATE()) AS date)) "
            "OR (DTAPGTO IS NOT NULL AND CAST(DTAPGTO AS date) >= CAST(DATEADD(day,-120,GETDATE()) AS date))"
        ),
        "query": (
            "SELECT c.*, "
            "CAST(c.DTACONTA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) FROM (VALUES "
            "(CAST(c.DTACONTA AS datetime2)), "
            f"(NULLIF(CAST(c.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2))), "
            "(CAST(c.DTAPGTO AS datetime2))"
            ") AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.CONTASRECEBER c"
        ),
        "enabled": True,
    },
    "contasreceberbaixa": {
        "table": "dbo.CONTASRECEBERBAIXA",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        "query": (
            "SELECT c.*, "
            "CAST(c.DATABAIXA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) FROM (VALUES "
            "(CAST(c.DATABAIXA AS datetime2)), "
            f"(NULLIF(CAST(c.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            ") AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.CONTASRECEBERBAIXA c"
        ),
        "enabled": True,
    },
    "contaspagarbaixa": {
        "table": "dbo.CONTASPAGARBAIXA",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        "query": (
            "SELECT c.*, "
            "CAST(c.DATABAIXA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "CAST(c.DATAREPL AS datetime2) AS TORQMIND_WATERMARK "
            "FROM dbo.CONTASPAGARBAIXA c"
        ),
        "enabled": True,
    },
    "movlctos": {
        "table": "dbo.MOVLCTOS",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        # DRE Xpert usa DTACONTA; DATAREPL/DATA sozinhos perdem competência do mês.
        "revisit_open_clause": (
            "CAST(DTACONTA AS date) >= CAST(DATEADD(day,-120,GETDATE()) AS date)"
        ),
        "query": (
            "SELECT m.*, "
            "CAST(m.DTACONTA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) FROM (VALUES "
            "(CAST(m.DTACONTA AS datetime2)), "
            "(CAST(m.DATA AS datetime2)), "
            f"(NULLIF(CAST(m.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            ") AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.MOVLCTOS m"
        ),
        "enabled": True,
    },
    "financeiro": {"table": "dbo.FINANCEIRO", "enabled": False},
    # Financeiro: controle de cheques recebidos (a compensar / depositado /
    # compensado / devolvido com motivo) + catalogo de situacoes (motivos).
    "cheques": {
        "table": "dbo.CHEQUESRECEBIDOS",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        # O cheque muda depois de recebido (compensacao seta DTACOMPENSADO;
        # devolucao seta ID_SITUACOES) e o ERP nem sempre mexe em DATAREPL. Rede
        # de seguranca: reler os NAO compensados recentes + os recem-compensados.
        "revisit_open_clause": (
            "(DTACOMPENSADO IS NULL AND CAST(DTACONTA AS date) >= CAST(DATEADD(day,-180,GETDATE()) AS date)) "
            "OR (DTACOMPENSADO IS NOT NULL AND CAST(DTACOMPENSADO AS date) >= CAST(DATEADD(day,-120,GETDATE()) AS date))"
        ),
        "query": (
            "SELECT c.*, "
            "CAST(c.DTACONTA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) FROM (VALUES "
            "(CAST(c.DTACONTA AS datetime2)), "
            f"(NULLIF(CAST(c.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2))), "
            "(CAST(c.DTACOMPENSADO AS datetime2))"
            ") AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.CHEQUESRECEBIDOS c"
        ),
        "enabled": True,
    },
    "situacoes": {
        "table": "dbo.SITUACOES",
        "watermark_column": "ID_SITUACOES",
        "watermark_order_by": "ID_SITUACOES, ID_FILIAL",
        "full_refresh": True,
        "enabled": True,
    },
    # Antifraude: lancamentos de credito de clientes (injecao/aplicacao) + saldo.
    "movcreditoentidades": {
        "table": "dbo.MOVCREDITOENTIDADES",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        "query": (
            "SELECT m.*, "
            "CAST(m.DATA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) FROM (VALUES "
            "(CAST(m.DATA AS datetime2)), "
            f"(NULLIF(CAST(m.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            ") AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.MOVCREDITOENTIDADES m"
        ),
        "enabled": True,
    },
    "credito": {
        "table": "dbo.CREDITO",
        "watermark_column": "ID_CREDITO",
        "watermark_order_by": "ID_CREDITO, ID_FILIAL",
        "full_refresh": True,
        "enabled": True,
    },
    # Vendas: abastecimentos do console da bomba (ticket medio de combustivel).
    "consolearquivo": {
        "table": "dbo.CONSOLEARQUIVO",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        "query": (
            "SELECT c.*, "
            "CAST(c.DATA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) FROM (VALUES "
            "(CAST(c.DATA AS datetime2)), "
            f"(NULLIF(CAST(c.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            ") AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.CONSOLEARQUIVO c"
        ),
        "enabled": True,
    },
    "nfe": {
        "table": "dbo.NFE",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_order_by": f"{WATERMARK_ALIAS}, ID_FILIAL, ID_DB, ID_COMPROVANTE, ID_NFE",
        "cursor_pk_columns": ["ID_FILIAL", "ID_DB", "ID_COMPROVANTE", "ID_NFE"],
        "contract_name": "nfe_pk_required_fields",
        "required_fields": ["ID_NFE", "ID_FILIAL", "ID_DB", "ID_COMPROVANTE", "STATUS"],
        "unique_key_fields": ["ID_FILIAL", "ID_DB", "ID_COMPROVANTE", "ID_NFE"],
        "preflight_tables": {
            "dbo.NFE": [
                "ID_NFE",
                "ID_FILIAL",
                "ID_DB",
                "ID_COMPROVANTE",
                "DATA",
                "STATUS",
            ],
        },
        "retention_days": COMMERCIAL_WINDOW_DAYS,
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT n.*, "
            "CAST(n.DATA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "CAST(n.DATA AS datetime2) AS TORQMIND_WATERMARK "
            "FROM dbo.NFE n"
        ),
        "enabled": True,
    },
    # Compliance ANP — fontes confirmadas no Xpert (CENTRALVR/ATXDADOS):
    # - preço bomba: LMC ⨝ LMCBICOS (PPL diário por bico/produto; sem ID_DB → 0)
    # - entrada/compra: COMPROVANTES(SAIDAS_ENTRADAS=1) ⨝ COMPENTRADAS ⨝ ITENSCOMPROVANTE
    "nfe_entrada": {
        "table": "dbo.COMPROVANTES",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_order_by": f"{WATERMARK_ALIAS}, ID_FILIAL, ID_DB, ID_COMPROVANTE",
        "cursor_pk_columns": ["ID_FILIAL", "ID_DB", "ID_COMPROVANTE"],
        "contract_name": "nfe_entrada_pk",
        "required_fields": ["ID_COMPROVANTE", "ID_FILIAL", "ID_DB"],
        "unique_key_fields": ["ID_FILIAL", "ID_DB", "ID_COMPROVANTE"],
        "preflight_tables": {
            "dbo.COMPROVANTES": ["ID_COMPROVANTE", "ID_FILIAL", "ID_DB", "DATA", "SAIDAS_ENTRADAS"],
            "dbo.COMPENTRADAS": ["ID_COMPROVANTE", "ID_FILIAL", "ID_DB", "CHAVEACESSONFE"],
        },
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT c.ID_FILIAL, c.ID_DB, c.ID_COMPROVANTE AS ID_NOTA, "
            "c.ID_COMPROVANTE, c.NROCOMPROVANTE AS NUMERO, "
            "ce.CHAVEACESSONFE AS CHAVEACESSO, ce.CHAVEACESSONFE AS CHAVEACESSONFE, "
            "ce.VLRFRETE, ce.VLRIPI, ce.IMPORTOU_XML, ce.CONFERIDA, "
            "CAST(c.DATA AS datetime2) AS DATAENTRADA, "
            "CAST(COALESCE(c.DTAEMISSAO, c.DATA) AS datetime2) AS DATA, "
            "CAST(c.DATA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) FROM (VALUES "
            "  (CAST(c.DATA AS datetime2)), "
            f"  (NULLIF(CAST(c.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2))), "
            f"  (NULLIF(CAST(ce.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            ") AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.COMPROVANTES c WITH (NOLOCK) "
            "INNER JOIN dbo.COMPENTRADAS ce WITH (NOLOCK) "
            "  ON ce.ID_FILIAL = c.ID_FILIAL AND ce.ID_DB = c.ID_DB "
            " AND ce.ID_COMPROVANTE = c.ID_COMPROVANTE "
            "WHERE ISNULL(c.SAIDAS_ENTRADAS, 0) = 1"
        ),
        "enabled": True,
    },
    "itens_nfe_entrada": {
        "table": "dbo.ITENSCOMPROVANTE",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_order_by": f"{WATERMARK_ALIAS}, ID_FILIAL, ID_DB, ID_COMPROVANTE, ID_ITENSCOMPROVANTE",
        "cursor_pk_columns": ["ID_FILIAL", "ID_DB", "ID_COMPROVANTE", "ID_ITENSCOMPROVANTE"],
        "contract_name": "itens_nfe_entrada_pk",
        "required_fields": ["ID_ITENSCOMPROVANTE", "ID_COMPROVANTE", "ID_FILIAL", "ID_DB", "ID_PRODUTOS"],
        "unique_key_fields": ["ID_FILIAL", "ID_DB", "ID_COMPROVANTE", "ID_ITENSCOMPROVANTE"],
        "preflight_tables": {
            "dbo.ITENSCOMPROVANTE": [
                "ID_ITENSCOMPROVANTE",
                "ID_FILIAL",
                "ID_DB",
                "ID_COMPROVANTE",
                "ID_PRODUTOS",
                "QTDE",
                "VLRCUSTO",
                "VLRUNITARIO",
            ],
            "dbo.COMPENTRADAS": ["ID_COMPROVANTE", "ID_FILIAL", "ID_DB"],
            "dbo.PRODUTOS": ["ID_PRODUTOS", "ID_FILIAL", "TIPOCOMBUSTIVEL", "NOMEPRODUTO"],
        },
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT i.ID_FILIAL, i.ID_DB, i.ID_COMPROVANTE AS ID_NOTA, "
            "i.ID_COMPROVANTE, i.ID_ITENSCOMPROVANTE AS ID_ITEM, "
            "i.ID_ITENSCOMPROVANTE, i.ID_PRODUTOS, i.QTDE AS QUANTIDADE, "
            "i.VLRCUSTO, i.VLRUNITARIO, i.VLRTOTALITEM, i.VLRVENDAENTRADA, "
            "COALESCE(NULLIF(i.VLRCUSTO, 0), i.VLRUNITARIO) AS CUSTO_UNITARIO, "
            "p.NOMEPRODUTO, p.TIPOCOMBUSTIVEL, p.CODIGOANP, p.UNIDADE, "
            "CAST(c.DATA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) FROM (VALUES "
            "  (CAST(c.DATA AS datetime2)), "
            f"  (NULLIF(CAST(c.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            ") AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.ITENSCOMPROVANTE i WITH (NOLOCK) "
            "INNER JOIN dbo.COMPROVANTES c WITH (NOLOCK) "
            "  ON c.ID_FILIAL = i.ID_FILIAL AND c.ID_DB = i.ID_DB "
            " AND c.ID_COMPROVANTE = i.ID_COMPROVANTE "
            "INNER JOIN dbo.COMPENTRADAS ce WITH (NOLOCK) "
            "  ON ce.ID_FILIAL = i.ID_FILIAL AND ce.ID_DB = i.ID_DB "
            " AND ce.ID_COMPROVANTE = i.ID_COMPROVANTE "
            "INNER JOIN dbo.PRODUTOS p WITH (NOLOCK) "
            "  ON p.ID_FILIAL = i.ID_FILIAL AND p.ID_PRODUTOS = i.ID_PRODUTOS "
            "WHERE ISNULL(c.SAIDAS_ENTRADAS, 0) = 1 "
            "  AND ("
            "    ISNULL(p.TIPOCOMBUSTIVEL, 0) > 0 "
            "    OR ("
            "      NULLIF(LTRIM(RTRIM(CAST(p.CODIGOANP AS varchar(32)))), '') IS NOT NULL "
            "      AND UPPER(LTRIM(RTRIM(CAST(p.UNIDADE AS varchar(16))))) = 'LT'"
            "    ) "
            "    OR p.NOMEPRODUTO LIKE 'GASOLINA%' "
            "    OR p.NOMEPRODUTO LIKE 'ETANOL%' "
            "    OR p.NOMEPRODUTO LIKE 'OLEO DIESEL%' "
            "    OR p.NOMEPRODUTO LIKE 'DIESEL%'"
            "  )"
        ),
        "enabled": True,
    },
    "preco_bomba_hist": {
        "table": "dbo.LMCBICOS",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_order_by": f"{WATERMARK_ALIAS}, ID_FILIAL, ID_LMCBICOS",
        "cursor_pk_columns": ["ID_FILIAL", "ID_LMCBICOS"],
        "contract_name": "preco_bomba_hist_pk",
        "required_fields": ["ID_LMCBICOS", "ID_FILIAL", "ID_PRODUTOS", "PPL"],
        "unique_key_fields": ["ID_FILIAL", "ID_LMCBICOS"],
        "preflight_tables": {
            "dbo.LMC": ["ID_LMC", "ID_FILIAL", "ID_PRODUTOS", "DTACONTA"],
            "dbo.LMCBICOS": ["ID_LMCBICOS", "ID_FILIAL", "ID_LMC", "ID_BICOS", "PPL"],
        },
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT "
            "  lb.ID_FILIAL, "
            "  CAST(0 AS int) AS ID_DB, "
            "  l.ID_PRODUTOS AS ID_PRODUTOS, "
            "  lb.ID_LMCBICOS AS ID_EVENTO, "
            "  lb.ID_LMCBICOS, "
            "  lb.ID_LMC, "
            "  lb.ID_BICOS, "
            "  lb.PPL AS PRECO, "
            "  lb.PPL AS PPL, "
            "  lb.VENDAS, "
            "  p.NOMEPRODUTO, "
            "  CAST(l.DTACONTA AS datetime2) AS DATAALTERACAO, "
            "  CAST(l.DTACONTA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) FROM (VALUES "
            "  (CAST(l.DTACONTA AS datetime2)), "
            f"  (NULLIF(CAST(l.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2))), "
            f"  (NULLIF(CAST(lb.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            ") AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.LMCBICOS lb WITH (NOLOCK) "
            "INNER JOIN dbo.LMC l WITH (NOLOCK) "
            "  ON l.ID_FILIAL = lb.ID_FILIAL AND l.ID_LMC = lb.ID_LMC "
            "LEFT JOIN dbo.PRODUTOS p WITH (NOLOCK) "
            "  ON p.ID_FILIAL = l.ID_FILIAL AND p.ID_PRODUTOS = l.ID_PRODUTOS "
            "WHERE ISNULL(lb.PPL, 0) > 0"
        ),
        "enabled": True,
    },
    # Antifraude: auditoria de troca de forma de pagamento.
    # CONTROLE_TROCA_PGTO = quem/quando trocou (DATA = data da troca);
    # aponta para o lancamento financeiro ANTIGO via ID_MOVLCTOSCANCELADOS.
    "controle_troca_pgto": {
        "table": "dbo.CONTROLE_TROCA_PGTO",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_order_by": f"{WATERMARK_ALIAS}, ID_FILIAL, ID, ID_DB",
        "cursor_pk_columns": ["ID_FILIAL", "ID", "ID_DB"],
        "contract_name": "controle_troca_pgto_pk",
        "required_fields": ["ID", "ID_FILIAL", "ID_DB"],
        "unique_key_fields": ["ID_FILIAL", "ID", "ID_DB"],
        "preflight_tables": {
            "dbo.CONTROLE_TROCA_PGTO": [
                "ID",
                "ID_FILIAL",
                "ID_DB",
                "ID_MOVLCTOSCANCELADOS",
                "USUARIO",
                "DATA",
            ],
        },
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT t.*, "
            "CAST(t.DATA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) "
            "   FROM (VALUES "
            "       (CAST(t.DATA AS datetime2)), "
            f"       (NULLIF(CAST(t.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            "   ) AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.CONTROLE_TROCA_PGTO t"
        ),
        "enabled": True,
    },
    # Antifraude: lancamentos financeiros CANCELADOS (forma DE da troca).
    # A "forma" e a conta ID_PLANODECONTAS (-> PLANODECONTAS.NOMEPLANODECONTAS).
    # DTACONTA = data contabil do lancamento; TIPO aqui = debito/credito (NAO tipo_forma).
    "movlctoscancelados": {
        "table": "dbo.MOVLCTOSCANCELADOS",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_order_by": f"{WATERMARK_ALIAS}, ID_FILIAL, ID_MOVLCTOSCANCELADOS, ID_DB",
        "cursor_pk_columns": ["ID_FILIAL", "ID_MOVLCTOSCANCELADOS", "ID_DB"],
        "contract_name": "movlctoscancelados_pk",
        "required_fields": ["ID_MOVLCTOSCANCELADOS", "ID_FILIAL", "ID_DB", "ID_PLANODECONTAS"],
        "unique_key_fields": ["ID_FILIAL", "ID_MOVLCTOSCANCELADOS", "ID_DB"],
        "preflight_tables": {
            "dbo.MOVLCTOSCANCELADOS": [
                "ID_MOVLCTOSCANCELADOS",
                "ID_FILIAL",
                "ID_DB",
                "ID_PLANODECONTAS",
                "REFERENCIA",
                "VALOR",
                "DTACONTA",
                "ID_TURNOS",
            ],
        },
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT m.*, "
            "CAST(m.DTACONTA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) "
            "   FROM (VALUES "
            "       (CAST(m.DTACONTA AS datetime2)), "
            f"       (NULLIF(CAST(m.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            "   ) AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.MOVLCTOSCANCELADOS m"
        ),
        "enabled": True,
    },
    # --- Estoque para a aba Solvencia (DRE) ---
    # Combustivel: o estoque real vem do SENSOR DE NIVEL DO TANQUE (dbo.MOVTANQUES.LEITURA),
    # nao das tabelas de estoque (que ficam negativas/absurdas). Loja: dbo.ESTOQUE.
    "tanques": {
        "table": "dbo.TANQUES",
        "watermark_column": "ID_TANQUES",
        "watermark_order_by": "ID_TANQUES, ID_FILIAL",
        "full_refresh": True,
        "enabled": True,
    },
    "movtanques": {
        "table": "dbo.MOVTANQUES",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "bootstrap_days": COMMERCIAL_WINDOW_DAYS,
        "query": (
            "SELECT m.*, "
            "CAST(m.DTACONTA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "CAST(m.DTACONTA AS datetime2) AS TORQMIND_WATERMARK "
            "FROM dbo.MOVTANQUES m"
        ),
        "enabled": True,
    },
    "estoque": {
        "table": "dbo.ESTOQUE",
        "watermark_column": "ID_ESTOQUE",
        "watermark_order_by": "ID_ESTOQUE, ID_FILIAL",
        "full_refresh": True,
        "enabled": True,
    },
    # --- Solvencia DRE: cartoes a receber (prazo de repasse) + banco + Havel ---
    # CONVENIOS: cadastro de operadora/bandeira com VENCIMENTO (dias de repasse),
    # ANTECIPACAO, DIASUTEIS. Join com FORMAS_PGTO_COMPROVANTES.ID_CARTAO.
    "convenios": {
        "table": "dbo.CONVENIOS",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": WATERMARK_ALIAS,
        "watermark_order_by": f"{WATERMARK_ALIAS}, ID_CONVENIOS, ID_FILIAL",
        "cursor_pk_columns": ["ID_CONVENIOS", "ID_FILIAL"],
        "full_refresh": True,
        "bootstrap_days": SOLVENCIA_BOOTSTRAP_DAYS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT c.*, "
            "COALESCE(CAST(c.DATAREPL AS datetime2), CAST('1900-01-01T00:00:00' AS datetime2)) "
            "AS TORQMIND_WATERMARK "
            "FROM dbo.CONVENIOS c"
        ),
        "enabled": True,
    },
    # MOVBANCOS: movimento bancario (TIPO/OPERACAO). Bootstrap desde Jan/2025.
    # Saldo as-of exige ID_DB = ID_FILIAL no ETL (ver docs/product/XPERT_BANCOS_MAP.md).
    "movbancos": {
        "table": "dbo.MOVBANCOS",
        "watermark_column": WATERMARK_ALIAS,
        "event_date_column": EVENT_DATE_ALIAS,
        "watermark_order_by": f"{WATERMARK_ALIAS}, ID_MOVBANCOS, ID_FILIAL, ID_DB",
        "cursor_pk_columns": ["ID_MOVBANCOS", "ID_FILIAL", "ID_DB"],
        "retention_days": SOLVENCIA_BOOTSTRAP_DAYS,
        "bootstrap_days": SOLVENCIA_BOOTSTRAP_DAYS,
        "watermark_overlap_seconds": DEFAULT_TEMPORAL_WATERMARK_OVERLAP_SECONDS,
        "query": (
            "SELECT m.*, "
            "CAST(m.DTACONTA AS datetime2) AS TORQMIND_DT_EVENTO, "
            "(SELECT MAX(v.dt) "
            "   FROM (VALUES "
            "       (CAST(m.DTACONTA AS datetime2)), "
            f"       (NULLIF(CAST(m.DATAREPL AS datetime2), CAST('{LEGACY_SENTINEL_DATETIME_SQL}' AS datetime2)))"
            "   ) AS v(dt)) AS TORQMIND_WATERMARK "
            "FROM dbo.MOVBANCOS m "
            "WHERE ISNULL(m.DELETAR, 0) = 0"
        ),
        "enabled": True,
    },
    # CONTASBANCARIA: cadastro de conta corrente (NK=ID_CONTASBANCARIAS+ID_FILIAL).
    "contasbancaria": {
        "table": "dbo.CONTASBANCARIA",
        "watermark_column": "ID_CONTASBANCARIAS",
        "watermark_order_by": "ID_CONTASBANCARIAS, ID_FILIAL",
        "cursor_pk_columns": ["ID_CONTASBANCARIAS", "ID_FILIAL"],
        "full_refresh": True,
        "enabled": True,
    },
    # BANCOSPADRAO: dominio FEBRABAN (nome do banco).
    "bancospadrao": {
        "table": "dbo.BANCOSPADRAO",
        "watermark_column": "ID_BANCOSPADRAO",
        "watermark_order_by": "ID_BANCOSPADRAO, ID_FILIAL",
        "cursor_pk_columns": ["ID_BANCOSPADRAO", "ID_FILIAL"],
        "full_refresh": True,
        "enabled": True,
    },
    # SALDOCLIENTES: snapshot de saldo por cliente (Havel = VALOR < 0 = posto credor).
    # Sem historico temporal nativo — full_refresh periodico.
    "saldoclientes": {
        "table": "dbo.SALDOCLIENTES",
        "watermark_column": "ID_SALDOCLIENTES",
        "watermark_order_by": "ID_SALDOCLIENTES, ID_FILIAL, ID_DB",
        "full_refresh": True,
        "enabled": True,
    },
}


@dataclass
class SQLServerConfig:
    dsn: Optional[str] = None
    driver: str = "ODBC Driver 18 for SQL Server"
    server: str = ""
    port: int = 1433
    database: str = ""
    user: str = ""
    password: str = ""
    encrypt: Optional[bool] = True
    trust_server_certificate: Optional[bool] = False
    login_timeout_seconds: int = 30


@dataclass
class APIConfig:
    base_url: str = "http://177.70.206.90:14023"
    route_prefix: str = "auto"
    ingest_key: Optional[str] = None
    empresa_id: Optional[int] = None
    idempotency_header: str = "X-Idempotency-Key"


@dataclass
class RuntimeConfig:
    batch_size: int = 5000
    fetch_size: int = 2000
    max_retries: int = 5
    timeout_seconds: int = 30
    connect_timeout_seconds: Optional[int] = None
    read_timeout_seconds: Optional[int] = None
    retry_backoff_base_seconds: float = 1.0
    retry_backoff_max_seconds: float = 30.0
    retry_jitter_seconds: float = 0.0
    batch_delay_seconds: float = 0.5
    gzip_enabled: bool = True
    state_dir: str = "state"
    spool_dir: str = "spool"
    spool_flush_max_files: int = 200
    interval_seconds: int = 60
    summary_log_file: str = "logs/torqmind-agent-summary.txt"
    log_level: str = "INFO"
    rescan_hourly_window_hours: int = 2
    rescan_daily_window_days: int = 7
    rescan_daily_after_hour: int = 0

    @property
    def effective_connect_timeout_seconds(self) -> int:
        return int(self.connect_timeout_seconds or self.timeout_seconds)

    @property
    def effective_read_timeout_seconds(self) -> int:
        return int(self.read_timeout_seconds or self.timeout_seconds)

    @property
    def request_timeout(self) -> tuple[int, int]:
        return (
            self.effective_connect_timeout_seconds,
            self.effective_read_timeout_seconds,
        )


@dataclass
class AppConfig:
    sqlserver: SQLServerConfig
    api: APIConfig
    runtime: RuntimeConfig
    datasets: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    id_empresa: Optional[int] = None
    id_db: Optional[int] = None


class AgentConfigError(RuntimeError):
    pass


def derive_encrypted_config_path(config_path: str | Path) -> Path:
    cfg_path = Path(config_path)
    if cfg_path.suffix.lower() == ".enc":
        return cfg_path
    return cfg_path.with_suffix(".enc")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _env_int(name: str, default: Optional[int]) -> Optional[int]:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _env_float(name: str, default: Optional[float]) -> Optional[float]:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def load_public_config(config_path: str | Path) -> Dict[str, Any]:
    return _load_yaml(Path(config_path))


def save_public_config(config_path: str | Path, raw: Dict[str, Any]) -> None:
    target = Path(config_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=False), encoding="utf-8")


def build_default_raw_config() -> Dict[str, Any]:
    return {
        "sqlserver": {
            "driver": "ODBC Driver 18 for SQL Server",
            "server": "",
            "port": 1433,
            "database": "",
            "user": "",
            "password": "",
            "encrypt": True,
            "trust_server_certificate": False,
            "login_timeout_seconds": 30,
        },
        "api": {
            "base_url": "http://177.70.206.90:14023",
            "route_prefix": "auto",
            "ingest_key": "",
            "empresa_id": 1,
            "idempotency_header": "X-Idempotency-Key",
        },
        "runtime": {
            "batch_size": 5000,
            "fetch_size": 2000,
            "max_retries": 5,
            "timeout_seconds": 30,
            "connect_timeout_seconds": 10,
            "read_timeout_seconds": 60,
            "retry_backoff_base_seconds": 1.0,
            "retry_backoff_max_seconds": 30.0,
            "retry_jitter_seconds": 0.0,
            "batch_delay_seconds": 0.5,
            "gzip_enabled": True,
            "state_dir": "state",
            "spool_dir": "spool",
            "spool_flush_max_files": 200,
            "interval_seconds": 60,
            "summary_log_file": "logs/torqmind-agent-summary.txt",
            "log_level": "INFO",
            "rescan_hourly_window_hours": 2,
            "rescan_daily_window_days": 7,
            "rescan_daily_after_hour": 0,
        },
        "id_empresa": 1,
        "id_db": 1,
        "datasets": copy.deepcopy(DEFAULT_DATASETS),
    }


def _merge_dataset_configs(user_cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    merged = {k: dict(v) for k, v in DEFAULT_DATASETS.items()}
    for ds, cfg in (user_cfg or {}).items():
        key = ds.strip().lower()
        base = merged.get(key, {"enabled": False})
        base.update(cfg or {})
        merged[key] = base
    return merged


def _apply_env_overrides(raw: Dict[str, Any]) -> Dict[str, Any]:
    sql = raw.setdefault("sqlserver", {})
    api = raw.setdefault("api", {})
    runtime = raw.setdefault("runtime", {})

    if "api_url" in raw and "base_url" not in api:
        api["base_url"] = raw["api_url"]

    for legacy_key in (
        "batch_size",
        "fetch_size",
        "max_retries",
        "timeout_seconds",
        "connect_timeout_seconds",
        "read_timeout_seconds",
        "retry_backoff_base_seconds",
        "retry_backoff_max_seconds",
        "retry_jitter_seconds",
        "batch_delay_seconds",
        "gzip_enabled",
        "state_dir",
        "spool_dir",
        "spool_flush_max_files",
        "interval_seconds",
        "summary_log_file",
        "log_level",
    ):
        if legacy_key in raw and legacy_key not in runtime:
            runtime[legacy_key] = raw[legacy_key]

    sql["dsn"] = os.getenv("TORQMIND_SQLSERVER_DSN", sql.get("dsn"))
    sql["driver"] = os.getenv("TORQMIND_SQLSERVER_DRIVER", sql.get("driver", "ODBC Driver 18 for SQL Server"))
    sql["server"] = os.getenv("TORQMIND_SQLSERVER_SERVER", sql.get("server"))
    sql["port"] = _env_int("TORQMIND_SQLSERVER_PORT", int(sql.get("port", 1433)))
    sql["database"] = os.getenv("TORQMIND_SQLSERVER_DATABASE", sql.get("database"))
    sql["user"] = os.getenv("TORQMIND_SQLSERVER_USER", sql.get("user"))
    sql["password"] = os.getenv("TORQMIND_SQLSERVER_PASSWORD", sql.get("password"))
    encrypt_env = os.getenv("TORQMIND_SQLSERVER_ENCRYPT")
    if encrypt_env is not None:
        sql["encrypt"] = encrypt_env.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    trust_env = os.getenv("TORQMIND_SQLSERVER_TRUST_SERVER_CERTIFICATE")
    if trust_env is not None:
        sql["trust_server_certificate"] = trust_env.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    sql["login_timeout_seconds"] = _env_int(
        "TORQMIND_SQLSERVER_LOGIN_TIMEOUT_SECONDS",
        int(sql.get("login_timeout_seconds", 30)),
    )

    api["base_url"] = os.getenv("TORQMIND_API_BASE_URL", api.get("base_url", "http://177.70.206.90:14023"))
    api["route_prefix"] = os.getenv("TORQMIND_API_ROUTE_PREFIX", api.get("route_prefix", "auto"))
    api["ingest_key"] = os.getenv("TORQMIND_INGEST_KEY", api.get("ingest_key"))
    api["idempotency_header"] = os.getenv(
        "TORQMIND_IDEMPOTENCY_HEADER",
        api.get("idempotency_header", "X-Idempotency-Key"),
    )
    empresa_env = os.getenv("TORQMIND_EMPRESA_ID")
    if empresa_env is not None:
        api["empresa_id"] = int(empresa_env)

    runtime["batch_size"] = _env_int("TORQMIND_BATCH_SIZE", int(runtime.get("batch_size", 5000)))
    runtime["fetch_size"] = _env_int("TORQMIND_FETCH_SIZE", int(runtime.get("fetch_size", 2000)))
    runtime["max_retries"] = _env_int("TORQMIND_MAX_RETRIES", int(runtime.get("max_retries", 5)))
    runtime["timeout_seconds"] = _env_int("TORQMIND_TIMEOUT_SECONDS", int(runtime.get("timeout_seconds", 30)))
    runtime["connect_timeout_seconds"] = _env_int(
        "TORQMIND_CONNECT_TIMEOUT_SECONDS",
        int(runtime.get("connect_timeout_seconds", runtime.get("timeout_seconds", 30)) or runtime.get("timeout_seconds", 30)),
    )
    runtime["read_timeout_seconds"] = _env_int(
        "TORQMIND_READ_TIMEOUT_SECONDS",
        int(runtime.get("read_timeout_seconds", runtime.get("timeout_seconds", 30)) or runtime.get("timeout_seconds", 30)),
    )
    runtime["retry_backoff_base_seconds"] = _env_float(
        "TORQMIND_RETRY_BACKOFF_BASE_SECONDS",
        float(runtime.get("retry_backoff_base_seconds", 1.0)),
    )
    runtime["retry_backoff_max_seconds"] = _env_float(
        "TORQMIND_RETRY_BACKOFF_MAX_SECONDS",
        float(runtime.get("retry_backoff_max_seconds", 30.0)),
    )
    runtime["retry_jitter_seconds"] = _env_float(
        "TORQMIND_RETRY_JITTER_SECONDS",
        float(runtime.get("retry_jitter_seconds", 0.0)),
    )
    runtime["batch_delay_seconds"] = _env_float(
        "TORQMIND_BATCH_DELAY_SECONDS",
        float(runtime.get("batch_delay_seconds", 0.5)),
    )
    runtime["gzip_enabled"] = _env_bool("TORQMIND_GZIP_ENABLED", bool(runtime.get("gzip_enabled", True)))
    runtime["state_dir"] = os.getenv("TORQMIND_STATE_DIR", runtime.get("state_dir", "state"))
    runtime["spool_dir"] = os.getenv("TORQMIND_SPOOL_DIR", runtime.get("spool_dir", "spool"))
    runtime["spool_flush_max_files"] = _env_int(
        "TORQMIND_SPOOL_FLUSH_MAX_FILES",
        int(runtime.get("spool_flush_max_files", 200)),
    )
    runtime["interval_seconds"] = _env_int(
        "TORQMIND_INTERVAL_SECONDS",
        int(runtime.get("interval_seconds", 60)),
    )
    runtime["summary_log_file"] = os.getenv(
        "TORQMIND_SUMMARY_LOG_FILE",
        runtime.get("summary_log_file", "logs/torqmind-agent-summary.txt"),
    )
    runtime["log_level"] = os.getenv("TORQMIND_LOG_LEVEL", runtime.get("log_level", "INFO"))

    raw["id_empresa"] = _env_int("TORQMIND_ID_EMPRESA", raw.get("id_empresa"))
    raw["id_db"] = _env_int("TORQMIND_ID_DB", raw.get("id_db"))

    enabled_env = os.getenv("TORQMIND_ENABLED_DATASETS")
    if enabled_env:
        enabled_set = {x.strip().lower() for x in enabled_env.split(",") if x.strip()}
        raw.setdefault("datasets", {})
        for ds in enabled_set:
            cur = raw["datasets"].get(ds, {})
            cur["enabled"] = True
            raw["datasets"][ds] = cur

    return raw


def _validate_required_fields(
    raw: Dict[str, Any],
    *,
    source_label: str,
    require_sql: bool = True,
    require_api_auth: bool = True,
    require_runtime: bool = True,
) -> None:
    sql = raw.get("sqlserver") or {}
    api = raw.get("api") or {}
    runtime = raw.get("runtime") or {}
    missing: list[str] = []

    if require_sql and not sql.get("dsn"):
        for key in ("server", "database", "user", "password"):
            if not sql.get(key):
                missing.append(f"sqlserver.{key}")
    if not api.get("base_url"):
        missing.append("api.base_url")
    if require_api_auth and api.get("empresa_id") is None and not api.get("ingest_key"):
        missing.append("api.ingest_key")
    if require_runtime and not runtime.get("interval_seconds"):
        missing.append("runtime.interval_seconds")

    if missing:
        raise AgentConfigError(f"Missing required field(s) in {source_label}: " + ", ".join(sorted(missing)))


def _fallback_config_path(path: Path) -> Path | None:
    """Locate an alternative config when the requested file is missing.

    After ``config migrate-from-yaml`` the plaintext ``.yaml`` is deleted and
    replaced by an encrypted ``.enc``. A service (or command) still pointing at
    the old ``.yaml`` — or at a differently named ``.enc`` — must keep running
    instead of failing with "Configuration file not found". We look for the
    same-stem sibling in the other format first, then for the canonical config
    names in the same directory, preferring the encrypted store.
    """
    parent = path.parent
    suffix = path.suffix.lower()
    candidates: list[Path] = []
    if suffix == ".enc":
        candidates += [path.with_suffix(".yaml"), path.with_suffix(".yml")]
    elif suffix in {".yaml", ".yml"}:
        candidates.append(path.with_suffix(".enc"))
    # Canonical encrypted configs take priority over plaintext.
    candidates += [
        parent / "config.enc",
        parent / "config.local.enc",
        parent / "config.yaml",
        parent / "config.local.yaml",
        parent / "config.yml",
    ]
    for candidate in candidates:
        if candidate != path and candidate.exists():
            return candidate
    return None


def load_raw_config(config_path: str | Path = "config.local.yaml") -> tuple[Dict[str, Any], Dict[str, Any]]:
    path = Path(config_path)
    if not path.exists():
        fallback = _fallback_config_path(path)
        if fallback is None:
            raise AgentConfigError(f"Configuration file not found: {path}")
        path = fallback

    if path.suffix.lower() == ".enc":
        try:
            raw = load_encrypted_json_file(path)
        except SecretStoreError as exc:
            raise AgentConfigError(str(exc)) from exc
        if not raw:
            raise AgentConfigError("Encrypted config is empty.")
        return raw, {"config_path": str(path), "kind": "encrypted"}

    raw = load_public_config(path)
    if not raw:
        raise AgentConfigError(f"YAML config is empty: {path}")
    return raw, {"config_path": str(path), "kind": "yaml"}


def save_encrypted_config(config_path: str | Path, raw: Dict[str, Any]) -> None:
    normalized = _apply_env_overrides(copy.deepcopy(raw))
    _validate_required_fields(normalized, source_label=str(config_path))
    save_encrypted_json_file(config_path, raw)


def load_config(
    config_path: str = "config.local.yaml",
    *,
    require_sql: bool = True,
    require_api_auth: bool = True,
    require_runtime: bool = True,
) -> AppConfig:
    raw, meta = load_raw_config(config_path)
    raw = _apply_env_overrides(copy.deepcopy(raw))
    _validate_required_fields(
        raw,
        source_label=meta["config_path"],
        require_sql=require_sql,
        require_api_auth=require_api_auth,
        require_runtime=require_runtime,
    )

    sql = SQLServerConfig(**(raw.get("sqlserver") or {}))
    api = APIConfig(**(raw.get("api") or {}))
    runtime_raw = raw.get("runtime") or {}
    runtime = RuntimeConfig(
        batch_size=int(runtime_raw.get("batch_size", 5000)),
        fetch_size=int(runtime_raw.get("fetch_size", 2000)),
        max_retries=int(runtime_raw.get("max_retries", 5)),
        timeout_seconds=int(runtime_raw.get("timeout_seconds", 30)),
        connect_timeout_seconds=(
            int(runtime_raw["connect_timeout_seconds"])
            if runtime_raw.get("connect_timeout_seconds") not in {None, ""}
            else None
        ),
        read_timeout_seconds=(
            int(runtime_raw["read_timeout_seconds"])
            if runtime_raw.get("read_timeout_seconds") not in {None, ""}
            else None
        ),
        retry_backoff_base_seconds=float(runtime_raw.get("retry_backoff_base_seconds", 1.0)),
        retry_backoff_max_seconds=float(runtime_raw.get("retry_backoff_max_seconds", 30.0)),
        retry_jitter_seconds=float(runtime_raw.get("retry_jitter_seconds", 0.0)),
        batch_delay_seconds=float(runtime_raw.get("batch_delay_seconds", 0.5)),
        gzip_enabled=bool(runtime_raw.get("gzip_enabled", True)),
        state_dir=str(runtime_raw.get("state_dir", "state")),
        spool_dir=str(runtime_raw.get("spool_dir", "spool")),
        spool_flush_max_files=int(runtime_raw.get("spool_flush_max_files", 200)),
        interval_seconds=int(runtime_raw.get("interval_seconds", 60)),
        summary_log_file=str(runtime_raw.get("summary_log_file", "logs/torqmind-agent-summary.txt")),
        log_level=str(runtime_raw.get("log_level", "INFO")),
        rescan_hourly_window_hours=int(runtime_raw.get("rescan_hourly_window_hours", 2)),
        rescan_daily_window_days=int(runtime_raw.get("rescan_daily_window_days", 7)),
        rescan_daily_after_hour=int(runtime_raw.get("rescan_daily_after_hour", 0)),
    )

    datasets = _merge_dataset_configs(raw.get("datasets") or {})

    if raw.get("id_empresa") is not None and api.empresa_id is None:
        api.empresa_id = int(raw["id_empresa"])

    return AppConfig(
        sqlserver=sql,
        api=api,
        runtime=runtime,
        datasets=datasets,
        id_empresa=raw.get("id_empresa"),
        id_db=raw.get("id_db"),
    )


def migrate_yaml_to_encrypted_config(
    yaml_path: str | Path,
    encrypted_path: str | Path | None = None,
    *,
    delete_source: bool = True,
) -> Dict[str, Any]:
    raw = load_public_config(yaml_path)
    if not raw:
        raise AgentConfigError("No YAML configuration found to migrate.")

    target = Path(encrypted_path) if encrypted_path else derive_encrypted_config_path(yaml_path)
    save_encrypted_config(target, raw)
    if delete_source:
        Path(yaml_path).unlink(missing_ok=True)

    return {
        "source": str(yaml_path),
        "target": str(target),
    }
