from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

try:
    import pyodbc  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    pyodbc = None

from agent.config import AppConfig
from agent.extractors.base import BaseExtractor, ExtractBatch
from agent.state.watermark import WatermarkStore


@dataclass
class QueryPlan:
    sql: str
    params: List
    query_mode: str
    watermark_column: str
    watermark_type_detected: str
    watermark_style: Optional[int]


class SQLServerExtractor(BaseExtractor):
    def __init__(self, cfg: AppConfig, logger) -> None:
        self.cfg = cfg
        self.logger = logger
        self.conn: Optional[pyodbc.Connection] = None

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

    def _detect_watermark_type(self, table: str, watermark_column: str) -> str:
        schema, table_name = self._split_table_name(table)
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
            """,
            (schema, table_name, watermark_column),
        )
        row = cur.fetchone()
        if not row:
            return "unknown"
        data_type = str(row[0]).strip().lower()
        if data_type in {"varchar", "nvarchar", "char", "nchar", "text", "ntext"}:
            return "text"
        if data_type in {"datetime", "datetime2", "smalldatetime", "date"}:
            return "datetime"
        return data_type

    @staticmethod
    def _quote_ident(ident: str) -> str:
        return f"[{str(ident).replace(']', ']]')}]"

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
    ) -> QueryPlan:
        ds_cfg = self._dataset_cfg(dataset)

        query = ds_cfg.get("query")
        wm_col = ds_cfg.get("watermark_column", "DATAREPL")
        query_mode = "param"

        params: List = []
        wm_expr = wm_col
        if watermark_type_detected == "text":
            style = watermark_style or 121
            wm_expr = f"TRY_CONVERT(datetime2, {wm_col}, {style})"
            query_mode = "try_convert"
        else:
            style = None

        if query:
            where_parts: List[str] = []
            if watermark_type_detected == "text":
                where_parts.append(f"{wm_expr} IS NOT NULL")
            if watermark_dt:
                where_parts.append(f"{wm_expr} > ?")
                params.append(watermark_dt)
            if dt_from:
                where_parts.append(f"{wm_expr} >= ?")
                params.append(dt_from)
            if dt_to:
                where_parts.append(f"{wm_expr} < ?")
                params.append(dt_to)

            if where_parts:
                lower_q = query.lower()
                if " where " in lower_q:
                    query = f"{query} AND {' AND '.join(where_parts)}"
                else:
                    query = f"{query} WHERE {' AND '.join(where_parts)}"
            return QueryPlan(
                sql=query,
                params=params,
                query_mode=query_mode,
                watermark_column=wm_col,
                watermark_type_detected=watermark_type_detected,
                watermark_style=style,
            )

        table = ds_cfg.get("table")
        if not table:
            raise ValueError(f"Missing table/query for dataset={dataset}")

        where_parts = []
        if watermark_type_detected == "text":
            where_parts.append(f"{wm_expr} IS NOT NULL")
        revisit_open_clause = str(ds_cfg.get("revisit_open_clause") or "").strip()
        if watermark_dt and revisit_open_clause:
            where_parts.append(f"({wm_expr} > ? OR ({revisit_open_clause}))")
            params.append(watermark_dt)
        elif watermark_dt:
            where_parts.append(f"{wm_expr} > ?")
            params.append(watermark_dt)
        if dt_from:
            where_parts.append(f"{wm_expr} >= ?")
            params.append(dt_from)
        if dt_to:
            where_parts.append(f"{wm_expr} < ?")
            params.append(dt_to)

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        sql = f"SELECT * FROM {table} {where_sql} ORDER BY {wm_col}"
        return QueryPlan(
            sql=sql,
            params=params,
            query_mode=query_mode,
            watermark_column=wm_col,
            watermark_type_detected=watermark_type_detected,
            watermark_style=style,
        )

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
        ds_cfg = self._dataset_cfg(dataset)
        wm_col = ds_cfg.get("watermark_column", "DATAREPL")
        table = ds_cfg.get("table", "<custom_query>")
        watermark_dt = WatermarkStore.parse_watermark_dt(watermark)
        wm_type = self._detect_watermark_type(table, wm_col) if table != "<custom_query>" else "unknown"
        styles = self._watermark_styles(ds_cfg) if wm_type == "text" else [None]

        self.logger.info(
            "dataset=%s watermark_column=%s watermark_value=%s watermark_type_detected=%s styles=%s from=%s to=%s",
            dataset,
            wm_col,
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
            )
            self.logger.info(
                "dataset=%s query_mode=%s watermark_style=%s sql=%s params=%s",
                dataset,
                plan.query_mode,
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
