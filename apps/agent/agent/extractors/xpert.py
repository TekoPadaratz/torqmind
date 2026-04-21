from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

try:
    import pyodbc  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    pyodbc = None

from agent_bkp.config import AppConfig, EVENT_DATE_ALIAS
from agent_bkp.extractors.base import BaseExtractor, ExtractBatch
from agent_bkp.state.watermark import WatermarkStore


@dataclass
class QueryPlan:
    sql: str
    params: List
    query_mode: str
    watermark_column: str
    watermark_expr: str
    event_date_expr: str
    watermark_type_detected: str
    watermark_style: Optional[int]


@dataclass(frozen=True)
class TableColumnInfo:
    name: str
    data_type: str


class SQLServerExtractor(BaseExtractor):
    def __init__(self, cfg: AppConfig, logger) -> None:
        self.cfg = cfg
        self.logger = logger
        self.conn: Optional[pyodbc.Connection] = None
        self._table_columns_cache: Dict[str, Dict[str, TableColumnInfo]] = {}

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
    def _is_legacy_datarepl(value: Optional[str]) -> bool:
        return str(value or "").strip().upper() == "DATAREPL"

    def _resolve_runtime_dataset_cfg(self, dataset: str) -> Dict[str, Any]:
        ds_cfg = dict(self._dataset_cfg(dataset))
        query = ds_cfg.get("query")
        configured_wm_col = str(ds_cfg.get("watermark_column", "DATAREPL") or "DATAREPL").strip()
        explicit_wm_expr = ds_cfg.get("watermark_expr")
        explicit_order = ds_cfg.get("watermark_order_by")
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
                if explicit_order in {None, ""}:
                    ds_cfg["watermark_order_by"] = str(event_date_expr).strip()
            return ds_cfg

        table = ds_cfg.get("table")
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
            if explicit_order in {None, ""}:
                ds_cfg["watermark_order_by"] = effective_wm_col

        if ds_cfg.get("event_date_expr") in {None, ""}:
            if preferred_date_info is not None:
                ds_cfg["event_date_column"] = preferred_date_info.name
                ds_cfg["event_date_expr"] = preferred_date_info.name

        return ds_cfg

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
                    item[col] = v.isoformat(timespec="seconds")
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
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
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

        params: List = []
        wm_expr = base_wm_expr
        if watermark_type_detected == "text":
            style = watermark_style or 121
            wm_expr = f"TRY_CONVERT(datetime2, {base_wm_expr}, {style})"
            query_mode = "try_convert"
        else:
            style = None
        if not event_date_expr:
            event_date_expr = wm_expr

        if query:
            outer_where_parts: List[str] = []
            if watermark_type_detected == "text":
                outer_where_parts.append(f"{wm_expr} IS NOT NULL")
            if watermark_dt:
                outer_where_parts.append(f"{wm_expr} > ?")
                params.append(watermark_dt)
            if dt_from:
                outer_where_parts.append(f"{event_date_expr} >= ?")
                params.append(dt_from)
            if dt_to:
                outer_where_parts.append(f"{event_date_expr} < ?")
                params.append(dt_to)

            base_query = self._clean_query(str(query))
            outer_where_sql = f" WHERE {' AND '.join(outer_where_parts)}" if outer_where_parts else ""
            order_expr = self._configured_expr(ds_cfg, "watermark_order_by", "watermark_column", fallback=wm_expr) or wm_expr
            return QueryPlan(
                sql=f"SELECT * FROM ({base_query}) AS src{outer_where_sql} ORDER BY {order_expr}",
                params=params,
                query_mode=query_mode,
                watermark_column=wm_col,
                watermark_expr=wm_expr,
                event_date_expr=event_date_expr,
                watermark_type_detected=watermark_type_detected,
                watermark_style=style,
            )

        table = ds_cfg.get("table")
        if not table:
            raise ValueError(f"Missing table/query for dataset={dataset}")

        where_parts = []
        if watermark_type_detected == "text":
            where_parts.append(f"{wm_expr} IS NOT NULL")
        if watermark_dt:
            where_parts.append(f"{wm_expr} > ?")
            params.append(watermark_dt)
        if dt_from:
            where_parts.append(f"{event_date_expr} >= ?")
            params.append(dt_from)
        if dt_to:
            where_parts.append(f"{event_date_expr} < ?")
            params.append(dt_to)

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        order_expr = self._configured_expr(ds_cfg, "watermark_order_by", "watermark_column", fallback=wm_expr) or wm_expr
        sql = f"SELECT * FROM {table} {where_sql} ORDER BY {order_expr}"
        return QueryPlan(
            sql=sql,
            params=params,
            query_mode=query_mode,
            watermark_column=wm_col,
            watermark_expr=wm_expr,
            event_date_expr=event_date_expr,
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
        order_expr = self._configured_expr(
            ds_cfg,
            "watermark_order_by",
            "watermark_column",
            fallback=", ".join(self._quote_ident(col) for col in key_columns),
        ) or ", ".join(self._quote_ident(col) for col in key_columns)
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
            return value.isoformat(timespec="microseconds")
        return WatermarkStore.normalize_watermark(str(value))

    def iter_batches(
        self,
        dataset: str,
        watermark: Optional[str],
        batch_size: int,
        fetch_size: int,
        dt_from: Optional[datetime] = None,
        dt_to: Optional[datetime] = None,
    ) -> Iterator[ExtractBatch]:
        ds_cfg = self._resolve_runtime_dataset_cfg(dataset)
        wm_col = ds_cfg.get("watermark_column", "DATAREPL")
        table = ds_cfg.get("table", "<custom_query>")
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
            "dataset=%s watermark_column=%s event_date_column=%s watermark_value=%s watermark_type_detected=%s styles=%s from=%s to=%s",
            dataset,
            wm_col,
            ds_cfg.get("event_date_column"),
            watermark,
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
                dt_from=dt_from,
                dt_to=dt_to,
                watermark_type_detected=wm_type,
                watermark_style=style,
                resolved_ds_cfg=ds_cfg,
            )
            self.logger.info(
                "dataset=%s query_mode=%s watermark_expr=%s event_date_expr=%s watermark_style=%s sql=%s params=%s",
                dataset,
                plan.query_mode,
                plan.watermark_expr,
                plan.event_date_expr,
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
            rows_in_style = 0

            while True:
                rows = cur.fetchmany(fetch_size)
                if not rows:
                    break

                for row in rows:
                    payload = {}
                    for cidx, col in enumerate(cols):
                        payload[col] = row[cidx]
                    batch.append(payload)
                    rows_in_style += 1
                    total_rows += 1

                    current = payload.get(wm_col)
                    current_iso = self._to_watermark_iso(current)
                    if current_iso is not None and (batch_wm is None or current_iso > batch_wm):
                        batch_wm = current_iso

                    if len(batch) >= batch_size:
                        yield ExtractBatch(rows=batch, max_watermark=batch_wm, extracted_at=datetime.utcnow())
                        batch = []

            if batch:
                yield ExtractBatch(rows=batch, max_watermark=batch_wm, extracted_at=datetime.utcnow())

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
