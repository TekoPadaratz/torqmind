#!/usr/bin/env python3
"""
Fix contasreceber sync: re-extracts all CONTASRECEBER from Xpert SQL Server
and updates STG + DW + Mart in PostgreSQL.

Root cause: The Agent bootstrap on 2026-05-22 captured DATAREPL=1900-01-01 titles
but DTAPGTO/VLRPAGO were NULL at that time. When clients paid in Xpert,
the incremental sync didn't capture the updates because DATAREPL didn't change.
"""
import sys
import pymssql
import psycopg2
import psycopg2.extras
import json
from datetime import datetime, date, timezone
from decimal import Decimal

# Connection configs
MSSQL_CONFIG = {
    "server": "172.30.0.12",
    "port": 1433,
    "user": "sa",
    "password": "XPT2000",
    "database": "ATXDADOS",
}

PG_CONFIG = {
    "host": "172.30.0.8",
    "port": 5432,
    "dbname": "torqmind",
    "user": "torqmind",
    "password": "ox6C7HxOsRqrwueJ8J3VoxsoBYKEDSx5lZP5j8rH4o",
}

ID_EMPRESA = 1


def json_serial(obj):
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    raise TypeError(f"Type {type(obj)} not serializable")


def extract_from_xpert():
    """Extract all CONTASRECEBER from Xpert SQL Server."""
    print("[1/5] Connecting to Xpert SQL Server...")
    conn = pymssql.connect(**MSSQL_CONFIG)
    cur = conn.cursor(as_dict=True)
    
    print("[1/5] Extracting CONTASRECEBER...")
    cur.execute("SELECT c.* FROM dbo.CONTASRECEBER c")
    rows = cur.fetchall()
    print(f"  Extracted {len(rows)} rows from CONTASRECEBER")
    
    conn.close()
    return rows


def extract_baixas_from_xpert():
    """Extract all CONTASRECEBERBAIXA from Xpert."""
    print("[1.5/5] Extracting CONTASRECEBERBAIXA...")
    conn = pymssql.connect(**MSSQL_CONFIG)
    cur = conn.cursor(as_dict=True)
    cur.execute("SELECT c.* FROM dbo.CONTASRECEBERBAIXA c")
    rows = cur.fetchall()
    print(f"  Extracted {len(rows)} rows from CONTASRECEBERBAIXA")
    conn.close()
    return rows


def upsert_stg(pg_conn, rows, table_name, id_col):
    """Upsert rows into stg table using ON CONFLICT on PK (id_empresa, id_filial, id_db, id_xxx)."""
    print(f"[2/5] Upserting {len(rows)} rows into stg.{table_name}...")
    cur = pg_conn.cursor()
    
    now = datetime.now(timezone.utc)
    batch_size = 1000
    upserted = 0
    
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        values = []
        for row in batch:
            id_filial = row["ID_FILIAL"]
            id_db = row.get("ID_DB", id_filial)
            id_record = row[id_col]
            payload = json.dumps(row, default=json_serial)
            values.append((ID_EMPRESA, id_filial, id_db, id_record, payload, now))
        
        psycopg2.extras.execute_values(
            cur,
            f"""INSERT INTO stg.{table_name} 
                (id_empresa, id_filial, id_db, id_{table_name}, payload, ingested_at)
                VALUES %s
                ON CONFLICT (id_empresa, id_filial, id_db, id_{table_name})
                DO UPDATE SET 
                    payload = EXCLUDED.payload,
                    ingested_at = EXCLUDED.ingested_at
            """,
            values,
            template="(%s, %s, %s, %s, %s::jsonb, %s)",
            page_size=batch_size
        )
        upserted += len(batch)
        if upserted % 10000 == 0:
            print(f"  ... {upserted}/{len(rows)}")
    
    pg_conn.commit()
    print(f"  Upserted {upserted} rows into stg.{table_name}")
    return upserted


def rebuild_dw_fact_financeiro(pg_conn):
    """Rebuild dw.fact_financeiro from STG."""
    print("[3/5] Rebuilding dw.fact_financeiro...")
    cur = pg_conn.cursor()
    
    # Truncate and rebuild
    cur.execute("TRUNCATE TABLE dw.fact_financeiro;")
    
    # Insert from contasreceber (tipo_titulo=1)
    cur.execute("""
        INSERT INTO dw.fact_financeiro (
            id_empresa, id_filial, id_db, tipo_titulo, id_titulo, id_entidade,
            data_emissao, data_key_emissao, vencimento, data_key_venc,
            data_pagamento, data_key_pgto, valor, valor_pago, payload,
            created_at, updated_at
        )
        SELECT 
            id_empresa,
            id_filial,
            COALESCE((payload->>'ID_DB')::int, id_filial) as id_db,
            1 as tipo_titulo,
            id_contasreceber as id_titulo,
            (payload->>'ID_ENTIDADE')::int as id_entidade,
            CASE WHEN payload->>'DTACONTA' IS NOT NULL AND payload->>'DTACONTA' != '' 
                 THEN (payload->>'DTACONTA')::timestamp::date END as data_emissao,
            CASE WHEN payload->>'DTACONTA' IS NOT NULL AND payload->>'DTACONTA' != ''
                 THEN TO_CHAR((payload->>'DTACONTA')::timestamp::date, 'YYYYMMDD')::int END as data_key_emissao,
            CASE WHEN payload->>'DTAVCTO' IS NOT NULL AND payload->>'DTAVCTO' != ''
                 THEN (payload->>'DTAVCTO')::timestamp::date END as vencimento,
            CASE WHEN payload->>'DTAVCTO' IS NOT NULL AND payload->>'DTAVCTO' != ''
                 THEN TO_CHAR((payload->>'DTAVCTO')::timestamp::date, 'YYYYMMDD')::int END as data_key_venc,
            CASE WHEN payload->>'DTAPGTO' IS NOT NULL AND payload->>'DTAPGTO' != '' 
                 AND (payload->>'DTAPGTO')::timestamp::date > '1900-01-01'
                 THEN (payload->>'DTAPGTO')::timestamp::date END as data_pagamento,
            CASE WHEN payload->>'DTAPGTO' IS NOT NULL AND payload->>'DTAPGTO' != ''
                 AND (payload->>'DTAPGTO')::timestamp::date > '1900-01-01'
                 THEN TO_CHAR((payload->>'DTAPGTO')::timestamp::date, 'YYYYMMDD')::int END as data_key_pgto,
            COALESCE((payload->>'VALOR')::numeric, 0) as valor,
            COALESCE((payload->>'VLRPAGO')::numeric, 0) as valor_pago,
            payload,
            ingested_at as created_at,
            NOW() as updated_at
        FROM stg.contasreceber
        WHERE id_empresa = 1;
    """)
    cnt = cur.rowcount
    print(f"  Inserted {cnt} rows into dw.fact_financeiro from contasreceber")
    
    pg_conn.commit()
    return cnt


def rebuild_mart_delinquency(pg_conn):
    """Rebuild mart.customer_delinquency_summary."""
    print("[4/5] Rebuilding mart.customer_delinquency_summary...")
    cur = pg_conn.cursor()
    
    cur.execute("TRUNCATE TABLE mart.customer_delinquency_summary;")
    
    cur.execute("""
        INSERT INTO mart.customer_delinquency_summary (
            id_empresa, id_filial, id_cliente, cliente_nome,
            titulos_ate_30d, valor_ate_30d,
            titulos_acima_30d, valor_acima_30d,
            titulos_a_vencer, valor_a_vencer,
            max_dias_atraso, valor_total_vencido,
            dt_ref, updated_at
        )
        SELECT 
            f.id_empresa,
            f.id_filial,
            f.id_entidade as id_cliente,
            COALESCE(
                (SELECT e.payload->>'FANTASIA' FROM stg.entidades e 
                 WHERE e.id_empresa = f.id_empresa AND e.id_entidade = f.id_entidade LIMIT 1),
                (SELECT e.payload->>'NOME' FROM stg.entidades e 
                 WHERE e.id_empresa = f.id_empresa AND e.id_entidade = f.id_entidade LIMIT 1),
                'Cliente ' || f.id_entidade
            ) as cliente_nome,
            COUNT(*) FILTER (WHERE f.vencimento < CURRENT_DATE AND (CURRENT_DATE - f.vencimento) <= 30) as titulos_ate_30d,
            COALESCE(SUM(f.valor - COALESCE(f.valor_pago, 0)) FILTER (WHERE f.vencimento < CURRENT_DATE AND (CURRENT_DATE - f.vencimento) <= 30), 0) as valor_ate_30d,
            COUNT(*) FILTER (WHERE f.vencimento < CURRENT_DATE AND (CURRENT_DATE - f.vencimento) > 30) as titulos_acima_30d,
            COALESCE(SUM(f.valor - COALESCE(f.valor_pago, 0)) FILTER (WHERE f.vencimento < CURRENT_DATE AND (CURRENT_DATE - f.vencimento) > 30), 0) as valor_acima_30d,
            COUNT(*) FILTER (WHERE f.vencimento >= CURRENT_DATE) as titulos_a_vencer,
            COALESCE(SUM(f.valor - COALESCE(f.valor_pago, 0)) FILTER (WHERE f.vencimento >= CURRENT_DATE), 0) as valor_a_vencer,
            COALESCE(MAX(CURRENT_DATE - f.vencimento) FILTER (WHERE f.vencimento < CURRENT_DATE), 0) as max_dias_atraso,
            COALESCE(SUM(f.valor - COALESCE(f.valor_pago, 0)) FILTER (WHERE f.vencimento < CURRENT_DATE), 0) as valor_total_vencido,
            CURRENT_DATE as dt_ref,
            NOW() as updated_at
        FROM dw.fact_financeiro f
        WHERE f.id_empresa = 1
          AND f.tipo_titulo = 1
          AND f.data_pagamento IS NULL
          AND (f.valor - COALESCE(f.valor_pago, 0)) > 0.01
        GROUP BY f.id_empresa, f.id_filial, f.id_entidade;
    """)
    cnt = cur.rowcount
    print(f"  Inserted {cnt} rows into mart.customer_delinquency_summary")
    
    pg_conn.commit()
    return cnt


def update_watermark(pg_conn):
    """Create/update watermark for contasreceber dataset."""
    print("[5/5] Updating watermark...")
    cur = pg_conn.cursor()
    now = datetime.now(timezone.utc)
    
    cur.execute("""
        INSERT INTO etl.watermark (id_empresa, dataset, last_ingested_at, updated_at, last_ts)
        VALUES (%s, 'contasreceber', %s, %s, %s)
        ON CONFLICT (id_empresa, dataset) 
        DO UPDATE SET last_ingested_at = EXCLUDED.last_ingested_at,
                      updated_at = EXCLUDED.updated_at,
                      last_ts = EXCLUDED.last_ts;
    """, (ID_EMPRESA, now, now, now))
    
    cur.execute("""
        INSERT INTO etl.watermark (id_empresa, dataset, last_ingested_at, updated_at, last_ts)
        VALUES (%s, 'contasreceberbaixa', %s, %s, %s)
        ON CONFLICT (id_empresa, dataset) 
        DO UPDATE SET last_ingested_at = EXCLUDED.last_ingested_at,
                      updated_at = EXCLUDED.updated_at,
                      last_ts = EXCLUDED.last_ts;
    """, (ID_EMPRESA, now, now, now))
    
    pg_conn.commit()
    print("  Watermark updated for contasreceber and contasreceberbaixa")


def verify_results(pg_conn):
    """Verify the fix by comparing with Xpert."""
    print("\n=== VERIFICATION ===")
    cur = pg_conn.cursor()
    
    # Check entity 7383
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE data_pagamento IS NULL AND (valor - COALESCE(valor_pago, 0)) > 0.01) as abertos,
            COALESCE(SUM(valor - COALESCE(valor_pago, 0)) FILTER (
                WHERE data_pagamento IS NULL AND (valor - COALESCE(valor_pago, 0)) > 0.01 AND vencimento < CURRENT_DATE
            ), 0) as vencido,
            COALESCE(SUM(valor - COALESCE(valor_pago, 0)) FILTER (
                WHERE data_pagamento IS NULL AND (valor - COALESCE(valor_pago, 0)) > 0.01 AND vencimento >= CURRENT_DATE
            ), 0) as a_vencer
        FROM dw.fact_financeiro
        WHERE id_empresa=1 AND id_filial=14122 AND id_entidade=7383 AND tipo_titulo=1
    """)
    row = cur.fetchone()
    print(f"\nEntidade 7383 (Transporte E.A.E):")
    print(f"  Total títulos: {row[0]}")
    print(f"  Abertos com saldo: {row[1]}")
    print(f"  VENCIDO: R$ {row[2]:,.2f}")
    print(f"  A VENCER: R$ {row[3]:,.2f}")
    
    # Compare with Xpert (known values)
    print(f"\n  Xpert real (SQL Server direto): Vencido=R$ 953,772.35, A vencer=R$ 24,846.16")
    print(f"  Diferença vencido: R$ {row[2] - Decimal('953772.35'):,.2f}")
    print(f"  Diferença a_vencer: R$ {row[3] - Decimal('24846.16'):,.2f}")


def main():
    print("=" * 60)
    print("FIX CONTASRECEBER SYNC")
    print("=" * 60)
    print(f"Started at: {datetime.now()}")
    
    # Extract from Xpert
    rows = extract_from_xpert()
    baixas = extract_baixas_from_xpert()
    
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(**PG_CONFIG)
    
    # Upsert into STG
    upsert_stg(pg_conn, rows, "contasreceber", "ID_CONTASRECEBER")
    upsert_stg(pg_conn, baixas, "contasreceberbaixa", "ID_CONTASRECEBERBAIXA")
    
    # Rebuild DW
    rebuild_dw_fact_financeiro(pg_conn)
    
    # Rebuild Mart
    rebuild_mart_delinquency(pg_conn)
    
    # Update watermark
    update_watermark(pg_conn)
    
    # Verify
    verify_results(pg_conn)
    
    pg_conn.close()
    print(f"\nCompleted at: {datetime.now()}")
    print("=" * 60)


if __name__ == "__main__":
    main()
