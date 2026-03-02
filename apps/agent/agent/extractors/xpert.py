from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterator, List, Optional

try:
    import pyodbc  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    pyodbc = None

from agent.config import AppConfig
from agent.extractors.base import BaseExtractor, ExtractBatch


class SQLServerExtractor(BaseExtractor):
    def __init__(self, cfg: AppConfig, logger) -> None:
        self.cfg = cfg
        self.logger = logger
        self.conn: Optional[pyodbc.Connection] = None

    def _connection_string(self) -> str:
        sql = self.cfg.sqlserver
        if sql.dsn:
            return f"DSN={sql.dsn};UID={sql.user};PWD={sql.password}"
        return (
            f"DRIVER={{{sql.driver}}};"
            f"SERVER={sql.server};"
            f"DATABASE={sql.database};"
            f"UID={sql.user};"
            f"PWD={sql.password}"
        )

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

    def _build_query(
        self,
        dataset: str,
        watermark: Optional[str],
        dt_from: Optional[datetime],
        dt_to: Optional[datetime],
    ) -> tuple[str, List]:
        ds_cfg = self._dataset_cfg(dataset)

        query = ds_cfg.get("query")
        wm_col = ds_cfg.get("watermark_column", "DATAREPL")

        params: List = []
        if query:
            where_parts: List[str] = []
            if watermark:
                where_parts.append(f"{wm_col} > ?")
                params.append(watermark)
            if dt_from:
                where_parts.append(f"{wm_col} >= ?")
                params.append(dt_from)
            if dt_to:
                where_parts.append(f"{wm_col} < ?")
                params.append(dt_to)

            if where_parts:
                lower_q = query.lower()
                if " where " in lower_q:
                    query = f"{query} AND {' AND '.join(where_parts)}"
                else:
                    query = f"{query} WHERE {' AND '.join(where_parts)}"
            return query, params

        table = ds_cfg.get("table")
        if not table:
            raise ValueError(f"Missing table/query for dataset={dataset}")

        where_parts = []
        if watermark:
            where_parts.append(f"{wm_col} > ?")
            params.append(watermark)
        if dt_from:
            where_parts.append(f"{wm_col} >= ?")
            params.append(dt_from)
        if dt_to:
            where_parts.append(f"{wm_col} < ?")
            params.append(dt_to)

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        sql = f"SELECT * FROM {table} {where_sql} ORDER BY {wm_col}"
        return sql, params

    def iter_batches(
        self,
        dataset: str,
        watermark: Optional[str],
        batch_size: int,
        fetch_size: int,
        dt_from: Optional[datetime] = None,
        dt_to: Optional[datetime] = None,
    ) -> Iterator[ExtractBatch]:
        sql, params = self._build_query(dataset, watermark, dt_from, dt_to)
        ds_cfg = self._dataset_cfg(dataset)
        wm_col = ds_cfg.get("watermark_column", "DATAREPL")

        conn = self._connect()
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]

        batch: List[dict] = []
        batch_wm: Optional[str] = None

        while True:
            rows = cur.fetchmany(fetch_size)
            if not rows:
                break

            for row in rows:
                payload = {}
                for idx, col in enumerate(cols):
                    val = row[idx]
                    payload[col] = val
                batch.append(payload)

                current = payload.get(wm_col)
                if current is not None:
                    current_s = str(current)
                    if batch_wm is None or current_s > batch_wm:
                        batch_wm = current_s

                if len(batch) >= batch_size:
                    yield ExtractBatch(rows=batch, max_watermark=batch_wm, extracted_at=datetime.utcnow())
                    batch = []

        if batch:
            yield ExtractBatch(rows=batch, max_watermark=batch_wm, extracted_at=datetime.utcnow())
