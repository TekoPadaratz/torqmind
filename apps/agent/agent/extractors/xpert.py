from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

try:
    import pyodbc  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    pyodbc = None

from agent.config import (
    AppConfig,
    EVENT_DATE_ALIAS,
    WATERMARK_ALIAS,
)
from agent.extractors.base import BaseExtractor, ExtractBatch
from agent.state.watermark import WatermarkStore
from agent.utils.timezone import business_datetime_iso, sqlserver_datetime_param


@dataclass
class QueryPlan:
    sql: str
    params: List
    query_mode: str
    watermark_column: str
    watermark_expr: str
    event_date_expr: str
    cursor_pk_columns: List[str]
    watermark_type_detected: str
    watermark_style: Optional[int]


@dataclass(frozen=True)
class TableColumnInfo:
    name: str
    data_type: str


class DatasetPreflightError(RuntimeError):
    pass


class SQLServerExtractor(BaseExtractor):
    def __init__(self, cfg: AppConfig, logger) -> None:
        self.cfg = cfg
        self.logger = logger
        self.conn: Optional[pyodbc.Connection] = None
        self._table_columns_cache: Dict[str, Dict[str, TableColumnInfo]] = {}
        self._table_primary_keys_cache: Dict[str, List[str]] = {}
        self._query_columns_cache: Dict[str, List[str]] = {}

    def _connection_string(self) -> str:
        sql = self.cfg.sqlserver
        server_value = str(sql.server or "").strip()
        if server_value and "," not in server_value and ":" not in server_value and sql.port:
            server_value = f"{server_value},{int(sql.port)}"

        if sql.dsn:
            parts = [f"DSN={sql.dsn}", f"UID={sql.user}", f"PWD={sql.password}"]
        else:
            parts = [
                f"DRIVER={{{sql.driver}}}",
                f"SERVER={server_value}",
                f"DATABASE={sql.database}",
                f"UID={sql.user}",
                f"PWD={sql.password}",
            ]

        if sql.encrypt is not None:
            parts.append(f"Encrypt={'yes' if sql.encrypt else 'no'}")
        if sql.trust_server_certificate is not None:
            parts.append(f"TrustServerCertificate={'yes' if sql.trust_server_certificate else 'no'}")
        parts.append(f"LoginTimeout={int(sql.login_timeout_seconds)}")
        return ";".join(parts)

    def _connect(self) -> pyodbc.Connection:
        if pyodbc is None:  # pragma: no cover
            raise RuntimeError("pyodbc is not installed. Install requirements and SQL Server ODBC driver first.")
        if self.conn is None:
            self.conn = pyodbc.connect(self._connection_string())
        return self.conn

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def check_connection(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()

    def _dataset_cfg(self, dataset: str) -> Dict:
        ds = dataset.lower()
        if ds not in self.cfg.datasets:
            raise ValueError(f"Dataset not configured: {dataset}")
        return self.cfg.datasets[ds]

    @staticmethod
    def _split_table_name(table: str) -> tuple[str, str]:
        clean = table.strip().replace("[", "").replace("]", "")
        if "." in clean:
            schema, name = clean.split(".", 1)
            return schema, name
        return "dbo", clean

    def _table_columns(self, table: str) -> Dict[str, TableColumnInfo]:
        cache_key = str(table).strip().lower()
        cached = self._table_columns_cache.get(cache_key)
        if cached is not None:
            return cached

        schema, table_name = self._split_table_name(table)
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """,
            (schema, table_name),
        )
        columns: Dict[str, TableColumnInfo] = {}
        for row in cur.fetchall():
            column_name = str(row[0]).strip()
            columns[column_name.lower()] = TableColumnInfo(
                name=column_name,
                data_type=str(row[1]).strip().lower(),
            )
        self._table_columns_cache[cache_key] = columns
        return columns

    def _table_primary_key_columns(self, table: str) -> List[str]:
        cache_key = str(table).strip().lower()
        cached = self._table_primary_keys_cache.get(cache_key)
        if cached is not None:
            return list(cached)

        try:
            schema, table_name = self._split_table_name(table)
            conn = self._connect()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT kcu.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                 AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                 AND tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE tc.TABLE_SCHEMA = ?
                  AND tc.TABLE_NAME = ?
                  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ORDER BY kcu.ORDINAL_POSITION
                """,
                (schema, table_name),
            )
            if hasattr(cur, "fetchall"):
                rows = cur.fetchall()
            else:
                rows = cur.fetchmany(100)
            columns = [str(row[0]).strip() for row in rows if str(row[0]).strip()]
        except Exception:
            columns = []
        self._table_primary_keys_cache[cache_key] = columns
        return list(columns)

    def _detect_watermark_type(self, table: str, watermark_column: str) -> str:
        column_info = self._table_columns(table).get(str(watermark_column or "").strip().lower())
        if not column_info:
            return "unknown"
        data_type = column_info.data_type
        if data_type in {"varchar", "nvarchar", "char", "nchar", "text", "ntext"}:
            return "text"
        if data_type in {"datetime", "datetime2", "smalldatetime", "date"}:
            return "datetime"
        return data_type

    @staticmethod
    def _quote_ident(ident: str) -> str:
        return f"[{str(ident).replace(']', ']]')}]"

    @staticmethod
    def _configured_expr(ds_cfg: Dict[str, Any], expr_key: str, column_key: str, fallback: Optional[str] = None) -> Optional[str]:
        expr = ds_cfg.get(expr_key)
        if expr not in {None, ""}:
            return str(expr).strip()
        column = ds_cfg.get(column_key)
        if column not in {None, ""}:
            return str(column).strip()
        return fallback

    @staticmethod
    def _clean_query(query: str) -> str:
        return str(query).strip().rstrip().rstrip(";")

    @staticmethod
    def _chunks(items: List[Dict[str, Any]], size: int) -> Iterator[List[Dict[str, Any]]]:
        chunk_size = max(1, int(size))
        for idx in range(0, len(items), chunk_size):
            yield items[idx : idx + chunk_size]

    @staticmethod
    def _normalize_pk_columns(value: Any) -> List[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if value in {None, ""}:
            return []
        return [str(value).strip()]

    @staticmethod
    def _normalize_row_aliases(value: Any) -> Dict[str, str]:
        if not isinstance(value, dict):
            return {}
        aliases: Dict[str, str] = {}
        for canonical_key, source_key in value.items():
            canonical = str(canonical_key or "").strip()
            source = str(source_key or "").strip()
            if canonical and source:
                aliases[canonical] = source
        return aliases

    @classmethod
    def _order_expr(cls, ds_cfg: Dict[str, Any], watermark_expr: str, cursor_pk_columns: List[str]) -> str:
        explicit_order = cls._configured_expr(ds_cfg, "watermark_order_by", "watermark_column")
        if explicit_order not in {None, ""}:
            return str(explicit_order).strip()
        if cursor_pk_columns:
            return ", ".join([str(watermark_expr).strip(), *cursor_pk_columns])
        return str(watermark_expr).strip()

    @staticmethod
    def _row_pk_tuple(row: Dict[str, Any], cursor_pk_columns: List[str]) -> Optional[list[Any]]:
        if not cursor_pk_columns:
            return None
        values: list[Any] = []
        for column in cursor_pk_columns:
            if column not in row:
                return None
            values.append(row.get(column))
        return values

    def _build_lexicographic_pk_predicate(self, pk_columns: List[str], pk_tuple: List[Any]) -> tuple[str, List[Any]]:
        if len(pk_columns) != len(pk_tuple):
            raise ValueError("cursor_pk_tuple length does not match cursor_pk_columns")

        or_parts: List[str] = []
        params: List[Any] = []
        prefix_columns: List[str] = []
        prefix_values: List[Any] = []
        for idx, column in enumerate(pk_columns):
            predicate_parts = [f"{self._quote_ident(prefix_col)} = ?" for prefix_col in prefix_columns]
            predicate_parts.append(f"{self._quote_ident(column)} > ?")
            or_parts.append("(" + " AND ".join(predicate_parts) + ")")
            params.extend(prefix_values)
            params.append(pk_tuple[idx])
            prefix_columns.append(column)
            prefix_values.append(pk_tuple[idx])
        return " OR ".join(or_parts), params

    @staticmethod
    def _is_legacy_datarepl(value: Optional[str]) -> bool:
        return str(value or "").strip().upper() == "DATAREPL"

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(timezone.utc)

    @classmethod
    def _utc_now_iso(cls, *, timespec: str = "seconds") -> str:
        return cls._utc_now().isoformat(timespec=timespec).replace("+00:00", "Z")

    @staticmethod
    def _normalize_preflight_tables(value: Any) -> Dict[str, List[str]]:
        if not isinstance(value, dict):
            return {}
        normalized: Dict[str, List[str]] = {}
        for table_name, columns in value.items():
            clean_table = str(table_name or "").strip()
            if not clean_table:
                continue
            if isinstance(columns, list):
                clean_columns = [str(column).strip() for column in columns if str(column).strip()]
            elif columns in {None, ""}:
                clean_columns = []
            else:
                clean_columns = [str(columns).strip()]
            normalized[clean_table] = clean_columns
        return normalized

    def _legacy_text_watermark_expr(self, base_wm_expr: str, style: int) -> str:
        normalized = f"NULLIF(LTRIM(RTRIM(CAST({base_wm_expr} AS varchar(64)))), '')"
        if int(style) == 103:
            iso_expr = (
                f"CASE WHEN {normalized} LIKE '[0-3][0-9]/[01][0-9]/[12][0-9][0-9][0-9]%' "
                f"THEN SUBSTRING({normalized}, 7, 4) + '-' + SUBSTRING({normalized}, 4, 2) + '-' + SUBSTRING({normalized}, 1, 2) "
                f"+ CASE WHEN LEN({normalized}) > 10 THEN SUBSTRING({normalized}, 11, LEN({normalized}) - 10) ELSE '' END "
                "ELSE NULL END"
            )
            return self._clean_query(
                f"CASE WHEN {iso_expr} IS NOT NULL AND ISDATE({iso_expr}) = 1 THEN CAST({iso_expr} AS datetime) ELSE NULL END"
            )

        canonical = f"REPLACE({normalized}, 'T', ' ')"
        return self._clean_query(
            f"CASE WHEN {canonical} IS NOT NULL AND {canonical} LIKE '[12][0-9][0-9][0-9]-%' AND ISDATE({canonical}) = 1 "
            f"THEN CONVERT(datetime, {canonical}, {int(style)}) ELSE NULL END"
        )

    @staticmethod
    def _apply_row_aliases(ds_cfg: Dict[str, Any], row: Dict[str, Any]) -> None:
        for canonical_key, source_key in SQLServerExtractor._normalize_row_aliases(ds_cfg.get("row_aliases")).items():
            if source_key in row:
                row[canonical_key] = row[source_key]

    def _resolve_runtime_dataset_cfg(self, dataset: str) -> Dict[str, Any]:
        ds_cfg = dict(self._dataset_cfg(dataset))
        query = ds_cfg.get("query")
        table = ds_cfg.get("table")
        configured_wm_col = str(ds_cfg.get("watermark_column", "DATAREPL") or "DATAREPL").strip()
        explicit_wm_expr = ds_cfg.get("watermark_expr")
        explicit_order = ds_cfg.get("watermark_order_by")
        cursor_pk_columns = self._normalize_pk_columns(ds_cfg.get("cursor_pk_columns"))
        if not cursor_pk_columns and table:
            cursor_pk_columns = self._table_primary_key_columns(str(table))
        if cursor_pk_columns:
            ds_cfg["cursor_pk_columns"] = cursor_pk_columns
        event_date_expr = self._configured_expr(
            ds_cfg,
            "event_date_expr",
            "event_date_column",
            fallback=(EVENT_DATE_ALIAS if query and EVENT_DATE_ALIAS in str(query).upper() else None),
        )

        # Query datasets cannot be introspected via INFORMATION_SCHEMA on the projected columns.
        # When still configured with the legacy DATAREPL watermark, prefer the event date alias.
        if query:
            if explicit_wm_expr in {None, ""} and self._is_legacy_datarepl(configured_wm_col) and event_date_expr:
                ds_cfg["watermark_column"] = str(ds_cfg.get("event_date_column") or event_date_expr).strip()
                ds_cfg["watermark_expr"] = str(event_date_expr).strip()
            wm_expr = self._configured_expr(ds_cfg, "watermark_expr", "watermark_column", fallback=configured_wm_col) or configured_wm_col
            ds_cfg["watermark_order_by"] = self._order_expr(ds_cfg, wm_expr, cursor_pk_columns)
            return ds_cfg

        if not table:
            return ds_cfg

        columns = self._table_columns(str(table))
        configured_info = columns.get(configured_wm_col.lower()) if configured_wm_col else None

        preferred_date_info: Optional[TableColumnInfo] = None
        event_date_column = str(ds_cfg.get("event_date_column") or "").strip()
        if event_date_column:
            preferred_date_info = columns.get(event_date_column.lower())
        if preferred_date_info is None:
            preferred_date_info = columns.get("data")

        if explicit_wm_expr in {None, ""}:
            effective_wm_col = configured_info.name if configured_info else configured_wm_col
            if self._is_legacy_datarepl(configured_wm_col) and preferred_date_info is not None:
                effective_wm_col = preferred_date_info.name
            elif configured_info is None and preferred_date_info is not None:
                effective_wm_col = preferred_date_info.name
            ds_cfg["watermark_column"] = effective_wm_col
            ds_cfg["watermark_expr"] = effective_wm_col

        if ds_cfg.get("event_date_expr") in {None, ""}:
            if preferred_date_info is not None:
                ds_cfg["event_date_column"] = preferred_date_info.name
                ds_cfg["event_date_expr"] = preferred_date_info.name

        wm_expr = self._configured_expr(ds_cfg, "watermark_expr", "watermark_column", fallback=configured_wm_col) or configured_wm_col
        ds_cfg["watermark_order_by"] = self._order_expr(ds_cfg, wm_expr, cursor_pk_columns)

        return ds_cfg

    def _require_table_columns(self, *, dataset: str, table: str, required_columns: List[str]) -> None:
        columns = self._table_columns(table)
        if not columns:
            raise DatasetPreflightError(
                f"Dataset `{dataset}` preflight failed: table `{table}` was not found or exposes no columns."
            )

        if not required_columns:
            return

        missing = [column for column in required_columns if str(column).strip().lower() not in columns]
        if missing:
            raise DatasetPreflightError(
                f"Dataset `{dataset}` preflight failed: table `{table}` is missing required column(s): "
                + ", ".join(missing)
                + "."
            )

    def preflight_dataset(self, dataset: str) -> None:
        ds_cfg = self._resolve_runtime_dataset_cfg(dataset)
        preflight_tables = self._normalize_preflight_tables(ds_cfg.get("preflight_tables"))
        table_name = str(ds_cfg.get("table") or "").strip()
        if table_name and table_name not in preflight_tables:
            preflight_tables[table_name] = []

        for required_table, required_columns in preflight_tables.items():
            self._require_table_columns(
                dataset=dataset,
                table=required_table,
                required_columns=required_columns,
            )

        self.logger.info(
            "dataset=%s phase=preflight_contract status=ok tables=%s",
            dataset,
            ",".join(sorted(preflight_tables)) if preflight_tables else "<none>",
        )

    def _sample_top_rows(self, schema_name: str, table_name: str, top_n: int = 5) -> List[Dict[str, Any]]:
        conn = self._connect()
        cur = conn.cursor()
        sql = (
            f"SELECT TOP ({int(top_n)}) * "
            f"FROM {self._quote_ident(schema_name)}.{self._quote_ident(table_name)}"
        )
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        samples: List[Dict[str, Any]] = []
        for row in cur.fetchmany(top_n):
            item: Dict[str, Any] = {}
            for idx, col in enumerate(cols):
                v = row[idx]
                if isinstance(v, datetime):
                    item[col] = business_datetime_iso(v, timespec="seconds")
                elif v is None:
                    item[col] = None
                else:
                    text = str(v)
                    item[col] = text[:200]
            samples.append(item)
        return samples

    def schema_scan(self, keywords: List[str], top_n: int = 5) -> Dict[str, Any]:
        if not keywords:
            raise ValueError("keywords must not be empty")

        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              s.name AS schema_name,
              t.name AS table_name,
              c.name AS column_name,
              ty.name AS data_type
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.columns c ON c.object_id = t.object_id
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            ORDER BY s.name, t.name, c.column_id
            """
        )

        table_map: Dict[str, Dict[str, Any]] = {}
        keys = [k.strip().lower() for k in keywords if k.strip()]
        for row in cur.fetchall():
            schema_name = str(row[0])
            table_name = str(row[1])
            column_name = str(row[2])
            data_type = str(row[3])
            fqtn = f"{schema_name}.{table_name}"
            rec = table_map.setdefault(
                fqtn,
                {
                    "schema": schema_name,
                    "table": table_name,
                    "score": 0,
                    "matched_keywords": set(),
                    "columns": [],
                },
            )
            rec["columns"].append({"name": column_name, "type": data_type})

            tname = table_name.lower()
            cname = column_name.lower()
            for kw in keys:
                if kw in tname:
                    rec["score"] += 4
                    rec["matched_keywords"].add(kw)
                if kw in cname:
                    rec["score"] += 2
                    rec["matched_keywords"].add(kw)

        candidates = [v for v in table_map.values() if v["score"] > 0]
        candidates.sort(key=lambda x: (x["score"], len(x["matched_keywords"])), reverse=True)

        top_candidates = candidates[:25]
        for rec in top_candidates:
            try:
                rec["sample_top5"] = self._sample_top_rows(rec["schema"], rec["table"], top_n=top_n)
            except Exception as exc:  # noqa: PERF203
                rec["sample_top5"] = []
                rec["sample_error"] = str(exc)
            rec["matched_keywords"] = sorted(list(rec["matched_keywords"]))

        return {
            "generated_at": self._utc_now_iso(timespec="seconds"),
            "keywords": keys,
            "candidates": top_candidates,
        }

    def _build_query_plan(
        self,
        dataset: str,
        watermark_dt: Optional[datetime],
        dt_from: Optional[datetime],
        dt_to: Optional[datetime],
        watermark_type_detected: str,
        watermark_style: Optional[int],
        resolved_ds_cfg: Optional[Dict[str, Any]] = None,
        cursor_pk_tuple: Optional[list[Any]] = None,
    ) -> QueryPlan:
        ds_cfg = resolved_ds_cfg or self._dataset_cfg(dataset)

        query = ds_cfg.get("query")
        wm_col = ds_cfg.get("watermark_column", "DATAREPL")
        base_wm_expr = self._configured_expr(ds_cfg, "watermark_expr", "watermark_column", fallback="DATAREPL") or "DATAREPL"
        event_date_expr = self._configured_expr(
            ds_cfg,
            "event_date_expr",
            "event_date_column",
            fallback=(EVENT_DATE_ALIAS if query and EVENT_DATE_ALIAS in str(query).upper() else None),
        )
        query_mode = "param"
        cursor_pk_columns = self._normalize_pk_columns(ds_cfg.get("cursor_pk_columns"))

        params: List = []
        wm_expr = base_wm_expr
        if watermark_type_detected == "text":
            style = watermark_style or 121
            wm_expr = self._legacy_text_watermark_expr(base_wm_expr, style)
            query_mode = "legacy_case_convert"
        else:
            style = None
        if not event_date_expr:
            event_date_expr = wm_expr

        def append_watermark_predicate(target_parts: List[str]) -> None:
            sql_watermark_dt = sqlserver_datetime_param(watermark_dt)
            if not sql_watermark_dt:
                return
            if cursor_pk_tuple and cursor_pk_columns:
                pk_sql, pk_params = self._build_lexicographic_pk_predicate(cursor_pk_columns, cursor_pk_tuple)
                target_parts.append(f"({wm_expr} > ? OR ({wm_expr} = ? AND ({pk_sql})))")
                params.extend([sql_watermark_dt, sql_watermark_dt, *pk_params])
                return
            target_parts.append(f"{wm_expr} > ?")
            params.append(sql_watermark_dt)

        if query:
            outer_where_parts: List[str] = []
            sql_dt_from = sqlserver_datetime_param(dt_from)
            sql_dt_to = sqlserver_datetime_param(dt_to)
            if watermark_type_detected == "text":
                outer_where_parts.append(f"{wm_expr} IS NOT NULL")
            append_watermark_predicate(outer_where_parts)
            if sql_dt_from:
                outer_where_parts.append(f"{event_date_expr} >= ?")
                params.append(sql_dt_from)
            if sql_dt_to:
                outer_where_parts.append(f"{event_date_expr} < ?")
                params.append(sql_dt_to)

            base_query = self._clean_query(str(query))
            outer_where_sql = f" WHERE {' AND '.join(outer_where_parts)}" if outer_where_parts else ""
            order_expr = self._order_expr(ds_cfg, wm_expr, cursor_pk_columns)
            return QueryPlan(
                sql=f"SELECT * FROM ({base_query}) AS src{outer_where_sql} ORDER BY {order_expr}",
                params=params,
                query_mode=query_mode,
                watermark_column=wm_col,
                watermark_expr=wm_expr,
                event_date_expr=event_date_expr,
                cursor_pk_columns=cursor_pk_columns,
                watermark_type_detected=watermark_type_detected,
                watermark_style=style,
            )

        table = ds_cfg.get("table")
        if not table:
            raise ValueError(f"Missing table/query for dataset={dataset}")

        where_parts: List[str] = []
        sql_dt_from = sqlserver_datetime_param(dt_from)
        sql_dt_to = sqlserver_datetime_param(dt_to)
        if watermark_type_detected == "text":
            where_parts.append(f"{wm_expr} IS NOT NULL")
        append_watermark_predicate(where_parts)
        if sql_dt_from:
            where_parts.append(f"{event_date_expr} >= ?")
            params.append(sql_dt_from)
        if sql_dt_to:
            where_parts.append(f"{event_date_expr} < ?")
            params.append(sql_dt_to)

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        order_expr = self._order_expr(ds_cfg, wm_expr, cursor_pk_columns)
        sql = f"SELECT * FROM {table} {where_sql} ORDER BY {order_expr}"
        return QueryPlan(
            sql=sql,
            params=params,
            query_mode=query_mode,
            watermark_column=wm_col,
            watermark_expr=wm_expr,
            event_date_expr=event_date_expr,
            cursor_pk_columns=cursor_pk_columns,
            watermark_type_detected=watermark_type_detected,
            watermark_style=style,
        )

    def fetch_rows_by_keys(
        self,
        dataset: str,
        *,
        key_columns: List[str],
        keys: List[Dict[str, Any]],
        fetch_size: int,
        query_chunk_size: int = 200,
    ) -> List[Dict[str, Any]]:
        if not keys:
            return []

        ds_cfg = self._resolve_runtime_dataset_cfg(dataset)
        query = ds_cfg.get("query")
        table = ds_cfg.get("table")
        if not query and not table:
            raise ValueError(f"Missing table/query for dataset={dataset}")

        base_query = self._clean_query(str(query)) if query else f"SELECT * FROM {table}"
        order_expr = self._order_expr(
            ds_cfg,
            self._configured_expr(ds_cfg, "watermark_expr", "watermark_column", fallback=", ".join(key_columns)) or ", ".join(key_columns),
            self._normalize_pk_columns(ds_cfg.get("cursor_pk_columns")),
        )
        conn = self._connect()
        rows_out: List[Dict[str, Any]] = []

        for key_chunk in self._chunks(list(keys), size=query_chunk_size):
            where_parts: List[str] = []
            params: List[Any] = []
            for key in key_chunk:
                clause_parts: List[str] = []
                for col in key_columns:
                    if col not in key:
                        raise ValueError(f"Missing key column `{col}` for dataset={dataset}")
                    clause_parts.append(f"{self._quote_ident(col)} = ?")
                    params.append(key[col])
                where_parts.append(f"({' AND '.join(clause_parts)})")

            sql = f"SELECT * FROM ({base_query}) AS src WHERE {' OR '.join(where_parts)} ORDER BY {order_expr}"
            self.logger.info(
                "dataset=%s phase=fetch_by_keys keys=%s sql=%s params=%s",
                dataset,
                len(key_chunk),
                sql,
                params,
            )
            cur = conn.cursor()
            cur.execute(sql, params)
            cols = [c[0] for c in cur.description]
            while True:
                fetched = cur.fetchmany(fetch_size)
                if not fetched:
                    break
                for row in fetched:
                    item: Dict[str, Any] = {}
                    for idx, col in enumerate(cols):
                        item[col] = row[idx]
                    self._apply_row_aliases(ds_cfg, item)
                    rows_out.append(item)

        return rows_out

    @staticmethod
    def _watermark_styles(ds_cfg: Dict) -> List[int]:
        cfg_style = ds_cfg.get("watermark_style")
        if cfg_style is not None:
            return [int(cfg_style)]
        return [121, 103]

    @staticmethod
    def _to_watermark_iso(value: object) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return business_datetime_iso(value, timespec="microseconds")
        return WatermarkStore.normalize_watermark(str(value))

    def iter_batches(
        self,
        dataset: str,
        watermark: Optional[str],
        batch_size: int,
        fetch_size: int,
        cursor_pk_tuple: Optional[list[Any]] = None,
        dt_from: Optional[datetime] = None,
        dt_to: Optional[datetime] = None,
    ) -> Iterator[ExtractBatch]:
        ds_cfg = self._resolve_runtime_dataset_cfg(dataset)
        wm_col = ds_cfg.get("watermark_column", "DATAREPL")
        table = ds_cfg.get("table", "<custom_query>")
        cursor_pk_columns = self._normalize_pk_columns(ds_cfg.get("cursor_pk_columns"))
        watermark_dt = WatermarkStore.parse_watermark_dt(watermark)
        wm_type = self._detect_watermark_type(table, wm_col) if table != "<custom_query>" else "unknown"
        styles = self._watermark_styles(ds_cfg) if wm_type == "text" else [None]

        configured_ds_cfg = self._dataset_cfg(dataset)
        configured_wm_col = configured_ds_cfg.get("watermark_column", "DATAREPL")
        if str(configured_wm_col).strip() != str(wm_col).strip():
            self.logger.info(
                "dataset=%s phase=watermark_resolved configured_watermark_column=%s effective_watermark_column=%s event_date_column=%s",
                dataset,
                configured_wm_col,
                wm_col,
                ds_cfg.get("event_date_column"),
            )

        self.logger.info(
            "dataset=%s watermark_column=%s event_date_column=%s watermark_value=%s cursor_pk_tuple=%s watermark_type_detected=%s styles=%s from=%s to=%s",
            dataset,
            wm_col,
            ds_cfg.get("event_date_column"),
            watermark,
            cursor_pk_tuple,
            wm_type,
            styles,
            dt_from,
            dt_to,
        )

        conn = self._connect()
        total_rows = 0

        for idx, style in enumerate(styles):
            plan = self._build_query_plan(
                dataset=dataset,
                watermark_dt=watermark_dt,
                cursor_pk_tuple=cursor_pk_tuple,
                dt_from=dt_from,
                dt_to=dt_to,
                watermark_type_detected=wm_type,
                watermark_style=style,
                resolved_ds_cfg=ds_cfg,
            )
            self.logger.info(
                "dataset=%s query_mode=%s watermark_expr=%s event_date_expr=%s cursor_pk_columns=%s watermark_style=%s sql=%s params=%s",
                dataset,
                plan.query_mode,
                plan.watermark_expr,
                plan.event_date_expr,
                plan.cursor_pk_columns,
                plan.watermark_style,
                plan.sql,
                plan.params,
            )
            cur = conn.cursor()
            try:
                cur.execute(plan.sql, plan.params)
            except Exception as exc:  # noqa: PERF203
                self.logger.exception(
                    "dataset=%s phase=query_error watermark_column=%s watermark_value=%s watermark_type_detected=%s watermark_style=%s suggestion=%s error=%s",
                    dataset,
                    plan.watermark_column,
                    watermark,
                    plan.watermark_type_detected,
                    plan.watermark_style,
                    "coluna texto: configure watermark_style=103 ou reset watermark; coluna datetime: validar formato ISO no state",
                    str(exc),
                )
                raise

            cols = [c[0] for c in cur.description]

            batch: List[dict] = []
            batch_wm: Optional[str] = None
            batch_pk_tuple: Optional[list[Any]] = None
            rows_in_style = 0

            while True:
                rows = cur.fetchmany(fetch_size)
                if not rows:
                    break

                for row in rows:
                    payload = {}
                    for cidx, col in enumerate(cols):
                        payload[col] = row[cidx]
                    self._apply_row_aliases(ds_cfg, payload)
                    batch.append(payload)
                    rows_in_style += 1
                    total_rows += 1

                    current = payload.get(wm_col)
                    current_iso = self._to_watermark_iso(current)
                    current_pk_tuple = self._row_pk_tuple(payload, cursor_pk_columns)
                    if current_iso is not None:
                        batch_wm = current_iso
                        batch_pk_tuple = current_pk_tuple

                    if len(batch) >= batch_size:
                        yield ExtractBatch(
                            rows=batch,
                            max_watermark=batch_wm,
                            extracted_at=self._utc_now(),
                            last_pk_tuple=batch_pk_tuple,
                        )
                        batch = []
                        batch_wm = None
                        batch_pk_tuple = None

            if batch:
                yield ExtractBatch(
                    rows=batch,
                    max_watermark=batch_wm,
                    extracted_at=self._utc_now(),
                    last_pk_tuple=batch_pk_tuple,
                )

            if rows_in_style > 0:
                return

            if idx < len(styles) - 1 and watermark_dt is not None:
                self.logger.warning(
                    "dataset=%s phase=watermark_style_fallback from_style=%s to_style=%s reason=0_rows_with_watermark",
                    dataset,
                    style,
                    styles[idx + 1],
                )

        if total_rows == 0:
            self.logger.info("dataset=%s phase=empty_result", dataset)
