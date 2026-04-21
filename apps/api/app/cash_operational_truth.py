from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from app.sales_semantics import (
    comercial_cfop_class_sql,
    comercial_cfop_direction_sql,
)

CASH_OPEN_RELATION = ("mart", "agg_caixa_turno_aberto")
CASH_PAYMENT_RELATION = ("mart", "agg_caixa_forma_pagamento")
ALERT_RELATION = ("mart", "alerta_caixa_aberto")

CASH_OPEN_REQUIRED_COLUMNS: tuple[str, ...] = (
    "usuario_source",
    "last_activity_ts",
    "horas_sem_movimento",
    "is_stale",
    "is_operational_live",
    "snapshot_ts",
)
CASH_PAYMENT_REQUIRED_COLUMNS: tuple[str, ...] = ("forma_category",)
ALERT_REQUIRED_COLUMNS: tuple[str, ...] = ("last_activity_ts",)

EXPECTED_RUNTIME_RELATION_COLUMNS: tuple[tuple[str, str, str], ...] = tuple(
    (schema_name, relation_name, column_name)
    for schema_name, relation_name, required_columns in (
        (*CASH_OPEN_RELATION, CASH_OPEN_REQUIRED_COLUMNS),
        (*CASH_PAYMENT_RELATION, CASH_PAYMENT_REQUIRED_COLUMNS),
        (*ALERT_RELATION, ALERT_REQUIRED_COLUMNS),
    )
    for column_name in required_columns
)


def relation_columns(conn, schema_name: str, relation_name: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT a.attname
        FROM pg_attribute a
        JOIN pg_class c
          ON c.oid = a.attrelid
        JOIN pg_namespace n
          ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s
          AND a.attnum > 0
          AND NOT a.attisdropped
        """,
        (schema_name, relation_name),
    ).fetchall()
    return {str(row["attname"] if isinstance(row, dict) else row[0]) for row in rows}


def missing_relation_columns(
    conn,
    schema_name: str,
    relation_name: str,
    required_columns: Iterable[str],
) -> list[str]:
    available = relation_columns(conn, schema_name, relation_name)
    return [str(column_name) for column_name in required_columns if str(column_name) not in available]


def missing_runtime_relation_columns(conn) -> list[str]:
    missing: list[str] = []
    for schema_name, relation_name, column_name in EXPECTED_RUNTIME_RELATION_COLUMNS:
        if missing_relation_columns(conn, schema_name, relation_name, (column_name,)):
            missing.append(f"{schema_name}.{relation_name}.{column_name}")
    return missing


def relation_exists(conn, schema_name: str, relation_name: str) -> bool:
    return bool(relation_columns(conn, schema_name, relation_name))


def cash_open_schema_mode(conn) -> str:
    missing = missing_relation_columns(conn, *CASH_OPEN_RELATION, CASH_OPEN_REQUIRED_COLUMNS)
    return "rich" if not missing else "legacy"


def cash_payment_relation_exists(conn) -> bool:
    return relation_exists(conn, *CASH_PAYMENT_RELATION)


def cash_payment_supports_category(conn) -> bool:
    return not missing_relation_columns(conn, *CASH_PAYMENT_RELATION, CASH_PAYMENT_REQUIRED_COLUMNS)


def _scope_filter_sql(column_name: str, value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        branch_ids = sorted({int(item) for item in value if item is not None})
        if not branch_ids:
            return "AND 1 = 0"
        if len(branch_ids) == 1:
            return f"AND {column_name} = {branch_ids[0]}"
        branch_values = ",".join(str(branch_id) for branch_id in branch_ids)
        return f"AND {column_name} = ANY(ARRAY[{branch_values}]::int[])"
    return f"AND {column_name} = {int(value)}"


def _turno_value_sql(payload_expr: str, id_turno_expr: str) -> str:
    return f"""
      COALESCE(
        NULLIF(trim({payload_expr}->>'TURNO'), ''),
        NULLIF(trim({payload_expr}->>'NO_TURNO'), ''),
        NULLIF(trim({payload_expr}->>'NUMTURNO'), ''),
        NULLIF(trim({payload_expr}->>'NR_TURNO'), ''),
        NULLIF(trim({payload_expr}->>'NROTURNO'), ''),
        NULLIF(trim({payload_expr}->>'TURNO_CAIXA'), ''),
        NULLIF(trim({payload_expr}->>'TURNOCAIXA'), ''),
        CASE
          WHEN {id_turno_expr} IS NOT NULL AND {id_turno_expr} > 0 THEN {id_turno_expr}::text
          ELSE NULL
        END
      )
    """


def _cash_eligible_sql(cash_expr: str, data_expr: str, data_conta_expr: str, id_turno_expr: str) -> str:
    return f"etl.resolve_cash_eligible({cash_expr}, {data_expr}, {data_conta_expr}, {id_turno_expr})"


def cash_open_source_sql(
    conn,
    *,
    id_empresa: int | None = None,
    id_filial: int | list[int] | None = None,
    alias: str = "a",
) -> tuple[str, str]:
    mode = cash_open_schema_mode(conn)
    tenant_filter = _scope_filter_sql("t.id_empresa", id_empresa)
    branch_filter = _scope_filter_sql("t.id_filial", id_filial)
    sql = f"""
      (
        WITH runtime AS (
          SELECT now() AS clock_ts
        ), open_turns AS (
          SELECT
            t.id_empresa,
            t.id_filial,
            t.id_turno,
            t.id_usuario,
            t.abertura_ts,
            t.fechamento_ts,
            t.payload
          FROM dw.fact_caixa_turno t
          WHERE t.is_aberto = true
            AND t.abertura_ts IS NOT NULL
            {tenant_filter}
            {branch_filter}
        ), vendas_turno AS (
          SELECT
            docs.id_empresa,
            docs.id_filial,
            docs.id_turno,
            MAX(docs.data) AS last_sale_ts,
            COALESCE(SUM(docs.total) FILTER (WHERE docs.cancelado = false AND docs.cfop_direction = 'saida'), 0)::numeric(18,2) AS total_vendas,
            COUNT(DISTINCT docs.doc_key) FILTER (WHERE docs.cancelado = false AND docs.cfop_direction = 'saida')::int AS qtd_vendas,
            COALESCE(SUM(docs.total) FILTER (WHERE docs.cancelado = true AND docs.cfop_direction IN ('saida', 'entrada')), 0)::numeric(18,2) AS total_cancelamentos,
            COUNT(DISTINCT docs.doc_key) FILTER (WHERE docs.cancelado = true AND docs.cfop_direction IN ('saida', 'entrada'))::int AS qtd_cancelamentos,
            COALESCE(SUM(docs.total) FILTER (WHERE docs.cancelado = false AND docs.cfop_class IN ('devolucao_saida', 'devolucao_entrada')), 0)::numeric(18,2) AS total_devolucoes,
            COUNT(DISTINCT docs.doc_key) FILTER (WHERE docs.cancelado = false AND docs.cfop_class IN ('devolucao_saida', 'devolucao_entrada'))::int AS qtd_devolucoes
          FROM (
            SELECT
              c.id_empresa,
              c.id_filial,
              COALESCE(c.id_turno, t.id_turno) AS id_turno,
              c.data,
              c.id_comprovante AS doc_key,
              COALESCE(c.cancelado, false) AS cancelado,
              {comercial_cfop_direction_sql('c')} AS cfop_direction,
              {comercial_cfop_class_sql('c')} AS cfop_class,
              COALESCE(c.valor_total, 0)::numeric(18,2) AS total
            FROM open_turns t
            JOIN dw.fact_comprovante c
              ON c.id_empresa = t.id_empresa
             AND c.id_filial = t.id_filial
             AND c.id_turno = t.id_turno
             AND {_cash_eligible_sql('c.cash_eligible', 'c.data', 'c.data_conta', 'c.id_turno')}
            WHERE {comercial_cfop_direction_sql('c')} IN ('saida', 'entrada')
          ) docs
          GROUP BY docs.id_empresa, docs.id_filial, docs.id_turno
        ), pagamentos_turno AS (
          SELECT
            p.id_empresa,
            p.id_filial,
            p.id_turno,
            MAX(p.dt_evento) AS last_payment_ts,
            COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total_pagamentos
          FROM open_turns t
          JOIN dw.fact_pagamento_comprovante p
            ON p.id_empresa = t.id_empresa
           AND p.id_filial = t.id_filial
           AND p.id_turno = t.id_turno
           AND p.dt_evento IS NOT NULL
           AND {_cash_eligible_sql('p.cash_eligible', 'p.dt_evento', 'p.data_conta', 'p.id_turno')}
          GROUP BY p.id_empresa, p.id_filial, p.id_turno
        )
        SELECT
          t.id_empresa,
          t.id_filial,
          COALESCE(f.nome, '') AS filial_nome,
          t.id_turno,
          {_turno_value_sql('t.payload', 't.id_turno')} AS turno_value,
          t.id_usuario,
          COALESCE(
            NULLIF(u.nome, ''),
            NULLIF(t.payload->>'NOMEUSUARIOS', ''),
            NULLIF(t.payload->>'NOME_USUARIOS', ''),
            NULLIF(t.payload->>'NOMEUSUARIO', ''),
            NULLIF(t.payload->>'NOME_USUARIO', ''),
            CASE WHEN t.id_usuario IS NOT NULL THEN format('Operador %%s', t.id_usuario) ELSE NULL END
          ) AS usuario_nome,
          CASE
            WHEN NULLIF(u.nome, '') IS NOT NULL THEN 'usuarios'
            WHEN COALESCE(
              NULLIF(t.payload->>'NOMEUSUARIOS', ''),
              NULLIF(t.payload->>'NOME_USUARIOS', ''),
              NULLIF(t.payload->>'NOMEUSUARIO', ''),
              NULLIF(t.payload->>'NOME_USUARIO', '')
            ) IS NOT NULL THEN 'turnos_payload'
            WHEN t.id_usuario IS NOT NULL THEN 'turno_id'
            ELSE 'indefinido'
          END AS usuario_source,
          t.abertura_ts,
          t.fechamento_ts,
          GREATEST(
            COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
            COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
            COALESCE(t.abertura_ts, '-infinity'::timestamptz)
          ) AS last_activity_ts,
          ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2)::numeric(10,2) AS horas_aberto,
          ROUND(
            EXTRACT(
              EPOCH FROM (
                runtime.clock_ts - GREATEST(
                  COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
                  COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
                  COALESCE(t.abertura_ts, '-infinity'::timestamptz)
                )
              )
            ) / 3600.0,
            2
          )::numeric(10,2) AS horas_sem_movimento,
          CASE
            WHEN GREATEST(
              COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
              COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
              COALESCE(t.abertura_ts, '-infinity'::timestamptz)
            ) < runtime.clock_ts - interval '96 hour' THEN true
            ELSE false
          END AS is_stale,
          CASE
            WHEN GREATEST(
              COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
              COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
              COALESCE(t.abertura_ts, '-infinity'::timestamptz)
            ) >= runtime.clock_ts - interval '96 hour' THEN true
            ELSE false
          END AS is_operational_live,
          CASE
            WHEN GREATEST(
              COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
              COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
              COALESCE(t.abertura_ts, '-infinity'::timestamptz)
            ) < runtime.clock_ts - interval '96 hour' THEN 'STALE'
            WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 24 THEN 'CRITICAL'
            WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 12 THEN 'HIGH'
            WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 6 THEN 'WARN'
            ELSE 'OK'
          END AS severity,
          CASE
            WHEN GREATEST(
              COALESCE(c.last_sale_ts, '-infinity'::timestamptz),
              COALESCE(p.last_payment_ts, '-infinity'::timestamptz),
              COALESCE(t.abertura_ts, '-infinity'::timestamptz)
            ) < runtime.clock_ts - interval '96 hour' THEN 'Stale'
            WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 24 THEN 'Crítico'
            WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 12 THEN 'Atenção alta'
            WHEN ROUND(EXTRACT(EPOCH FROM (runtime.clock_ts - t.abertura_ts)) / 3600.0, 2) >= 6 THEN 'Monitorar'
            ELSE 'Dentro da janela'
          END AS status_label,
          COALESCE(c.total_vendas, 0)::numeric(18,2) AS total_vendas,
          COALESCE(c.qtd_vendas, 0)::int AS qtd_vendas,
          COALESCE(c.total_cancelamentos, 0)::numeric(18,2) AS total_cancelamentos,
          COALESCE(c.qtd_cancelamentos, 0)::int AS qtd_cancelamentos,
          COALESCE(c.total_devolucoes, 0)::numeric(18,2) AS total_devolucoes,
          COALESCE(c.qtd_devolucoes, 0)::int AS qtd_devolucoes,
          COALESCE(p.total_pagamentos, 0)::numeric(18,2) AS total_pagamentos,
          runtime.clock_ts AS snapshot_ts,
          runtime.clock_ts AS updated_at
        FROM open_turns t
        CROSS JOIN runtime
        LEFT JOIN auth.filiais f
          ON f.id_empresa = t.id_empresa
         AND f.id_filial = t.id_filial
        LEFT JOIN dw.dim_usuario_caixa u
          ON u.id_empresa = t.id_empresa
         AND u.id_filial = t.id_filial
         AND u.id_usuario = t.id_usuario
        LEFT JOIN vendas_turno c
          ON c.id_empresa = t.id_empresa
         AND c.id_filial = t.id_filial
         AND c.id_turno = t.id_turno
        LEFT JOIN pagamentos_turno p
          ON p.id_empresa = t.id_empresa
         AND p.id_filial = t.id_filial
         AND p.id_turno = t.id_turno
      ) {alias}
    """
    return sql, mode


def apply_cash_operational_truth_migration(conn) -> None:
    migration_path = _resolve_cash_operational_truth_migration()
    conn.execute(migration_path.read_text(encoding="utf-8"))
    conn.commit()


def ensure_cash_operational_truth_schema(conn) -> bool:
    if not missing_runtime_relation_columns(conn):
        return False
    apply_cash_operational_truth_migration(conn)
    remaining = missing_runtime_relation_columns(conn)
    if remaining:
        raise RuntimeError(
            "Cash operational truth schema repair failed. Missing required columns: "
            + ", ".join(sorted(remaining))
        )
    return True


def _resolve_cash_operational_truth_migration() -> Path:
    here = Path(__file__).resolve()
    filename = "041_financial_semantics_operational_dashboards.sql"
    candidates: list[Path] = [parent / "sql" / "migrations" / filename for parent in here.parents]
    candidates.append(Path("/app/sql/migrations") / filename)

    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("Unable to locate 035_cash_operational_truth_schema_alignment.sql")
