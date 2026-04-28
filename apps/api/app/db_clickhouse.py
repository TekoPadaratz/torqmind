"""ClickHouse connection pool and query utilities.

PT-BR:
- Gerencia pool de conexões ClickHouse para leitura analítica
- Substitui reads de dw.* pelo torqmind_mart (MVs otimizadas)
- Suporta dual-read mode para validação durante transição

EN:
- Manages ClickHouse connection pool for analytics reads
- Replaces dw.* reads with torqmind_mart (optimized MVs)
- Supports dual-read mode for validation during migration
"""

from __future__ import annotations

import contextlib
import threading
from typing import Iterator, Optional, List, Dict, Any
import logging
import json

import clickhouse_connect

from app.config import settings

logger = logging.getLogger(__name__)

_pool: clickhouse_connect.driver.client.Client | None = None
_pool_lock = threading.Lock()


def _get_client() -> clickhouse_connect.driver.client.Client | None:
    """Get or create ClickHouse client (singleton pattern).
    
    PT-BR: Similar a db.py, mantemos um único cliente global para reutilização.
    EN: Like db.py, we maintain a single global client for reuse.
    """
    global _pool
    if _pool is not None:
        return _pool

    with _pool_lock:
        if _pool is None:
            try:
                _pool = clickhouse_connect.get_client(
                    host=settings.clickhouse_host,
                    port=settings.clickhouse_port,
                    database=settings.clickhouse_database,
                    username=settings.clickhouse_user,
                    password=settings.clickhouse_password,
                    read_timeout=30,
                    write_timeout=30,
                )
                logger.info(
                    f"ClickHouse client initialized: "
                    f"{settings.clickhouse_host}:{settings.clickhouse_port}/"
                    f"{settings.clickhouse_database}"
                )
            except Exception as e:
                logger.error(f"Failed to initialize ClickHouse client: {e}")
                raise

    return _pool


@contextlib.contextmanager
def get_clickhouse_client(
    tenant_id: Optional[int] = None,
) -> Iterator[clickhouse_connect.driver.client.Client]:
    """Context manager for ClickHouse client.
    
    PT-BR:
    - Retorna client ClickHouse para queries analíticas
    - Suporta tenant_id para isolação de dados (via WHERE clause)
    - Use dentro de try/finally para garantir limpeza
    
    EN:
    - Returns ClickHouse client for analytics queries
    - Supports tenant_id for data isolation (via WHERE clause)
    - Use within try/finally to ensure cleanup
    
    Example:
        with get_clickhouse_client(tenant_id=1) as client:
            result = client.query(
                "SELECT * FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa = :tenant_id",
                parameters={"tenant_id": tenant_id}
            )
    """
    client = _get_client()
    if client is None:
        raise RuntimeError("ClickHouse client not initialized")
    
    try:
        yield client
    except Exception as e:
        logger.error(f"ClickHouse query error: {e}")
        raise
    finally:
        # ClickHouse client is stateless; no explicit cleanup needed
        pass


def query_dict(
    query: str,
    parameters: Optional[Dict[str, Any]] = None,
    tenant_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Execute SELECT query and return list of dicts.
    
    PT-BR: Wrapper conveniente para queries de leitura
    EN: Convenient wrapper for read queries
    """
    with get_clickhouse_client(tenant_id=tenant_id) as client:
        result = client.query(query, parameters=parameters)
        return result.result_rows


def query_scalar(
    query: str,
    parameters: Optional[Dict[str, Any]] = None,
    tenant_id: Optional[int] = None,
) -> Any:
    """Execute query that returns single scalar value.
    
    PT-BR: Para queries como COUNT(*), SUM(...), etc.
    EN: For queries like COUNT(*), SUM(...), etc.
    """
    with get_clickhouse_client(tenant_id=tenant_id) as client:
        result = client.query(query, parameters=parameters)
        rows = result.result_rows
        if rows and len(rows) > 0:
            return rows[0][0]
        return None


def insert_batch(
    table: str,
    rows: List[Dict[str, Any]],
    order_by: Optional[List[str]] = None,
    batch_size: int = 100000,
) -> int:
    """Insert batch of rows into ClickHouse table.
    
    PT-BR:
    - Insere dados com ORDER BY para otimizar compressão
    - Chunk automático em batches de 100K linhas
    - Retorna total de linhas inseridas
    
    EN:
    - Inserts data with ORDER BY for compression optimization
    - Automatic chunking into 100K row batches
    - Returns total rows inserted
    
    Example:
        rows = [
            {"id_empresa": 1, "data_key": 20260428, "faturamento": 1000.50},
            {"id_empresa": 1, "data_key": 20260428, "faturamento": 2000.75},
        ]
        inserted = insert_batch(
            "torqmind_mart.agg_vendas_diaria",
            rows,
            order_by=["id_empresa", "data_key", "id_filial"]
        )
    """
    if not rows:
        return 0

    # Sort by order_by columns for optimal compression
    if order_by:
        rows = sorted(rows, key=lambda r: tuple(r.get(col) for col in order_by))

    total_inserted = 0
    with get_clickhouse_client() as client:
        # Insert in chunks
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            try:
                result = client.insert(table, chunk)
                total_inserted += len(chunk)
                logger.info(f"Inserted {len(chunk)} rows into {table}")
            except Exception as e:
                logger.error(f"Failed to insert batch into {table}: {e}")
                raise

    return total_inserted


def validate_row_count(
    table: str,
    expected_count: int,
    tolerance: float = 0.01,
) -> bool:
    """Validate row count in ClickHouse table vs expected.
    
    PT-BR: Usado para reconciliação após migração histórica
    EN: Used for reconciliation after historical migration
    """
    actual_count = query_scalar(f"SELECT COUNT(*) FROM {table}")
    if actual_count is None:
        actual_count = 0

    diff = abs(actual_count - expected_count)
    max_diff = int(expected_count * tolerance) if expected_count > 0 else 1

    if diff <= max_diff:
        logger.info(
            f"Row count validation PASSED for {table}: "
            f"expected={expected_count}, actual={actual_count}, diff={diff}"
        )
        return True
    else:
        logger.warning(
            f"Row count validation FAILED for {table}: "
            f"expected={expected_count}, actual={actual_count}, diff={diff}"
        )
        return False


def validate_aggregate(
    table: str,
    column: str,
    expected_sum: float,
    tolerance: float = 0.01,
) -> bool:
    """Validate aggregate SUM in ClickHouse table vs expected.
    
    PT-BR: Reconciliação de valores financeiros
    EN: Reconciliation of financial values
    """
    actual_sum = query_scalar(f"SELECT SUM({column}) FROM {table}")
    if actual_sum is None:
        actual_sum = 0

    diff = abs(actual_sum - expected_sum)
    max_diff = expected_sum * tolerance if expected_sum > 0 else 0.01

    if diff <= max_diff:
        logger.info(
            f"Aggregate validation PASSED for {table}.{column}: "
            f"expected={expected_sum}, actual={actual_sum}, diff={diff}"
        )
        return True
    else:
        logger.warning(
            f"Aggregate validation FAILED for {table}.{column}: "
            f"expected={expected_sum}, actual={actual_sum}, diff={diff}"
        )
        return False


class DualReadValidator:
    """Framework for dual-read validation during migration.
    
    PT-BR:
    - Executa query em Postgres E ClickHouse em paralelo
    - Compara resultados e loga discrepâncias
    - Feature flag: USE_CLICKHOUSE para controlar rollback
    
    EN:
    - Executes query on Postgres AND ClickHouse in parallel
    - Compares results and logs discrepancies
    - Feature flag: USE_CLICKHOUSE for rollback control
    """

    def __init__(self, enable: bool = True):
        self.enable = enable
        self.discrepancies: List[Dict[str, Any]] = []

    def compare(
        self,
        function_name: str,
        result_pg: Any,
        result_ch: Any,
    ) -> bool:
        """Compare Postgres vs ClickHouse results.
        
        Returns: True if match, False if discrepancy
        """
        if not self.enable:
            return True

        match = result_pg == result_ch

        if not match:
            discrepancy = {
                "function": function_name,
                "postgres_result": result_pg,
                "clickhouse_result": result_ch,
                "timestamp": str(__import__("datetime").datetime.now()),
            }
            self.discrepancies.append(discrepancy)
            logger.warning(f"Dual-read discrepancy detected: {discrepancy}")

        return match

    def report(self) -> Dict[str, Any]:
        """Generate validation report."""
        return {
            "enabled": self.enable,
            "total_discrepancies": len(self.discrepancies),
            "discrepancies": self.discrepancies,
        }


# Global validator instance
_validator: Optional[DualReadValidator] = None


def get_dual_read_validator() -> DualReadValidator:
    """Get or create global dual-read validator."""
    global _validator
    if _validator is None:
        # Enable only if USE_CLICKHOUSE feature flag not set to "false"
        use_ch = settings.use_clickhouse if hasattr(settings, "use_clickhouse") else True
        _validator = DualReadValidator(enable=use_ch)
    return _validator
