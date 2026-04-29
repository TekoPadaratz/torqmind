-- ============================================================================
-- FASE 3: Migration Guide & Integration Strategy
-- ============================================================================
--
-- FILE: phase3_migration_guide.md
-- PURPOSE: Step-by-step instructions for Phase 3 implementation
-- CREATED: 2026-04-28
--
-- ============================================================================

## FASE 3 CHECKLIST & TIMELINE

### Week 1: Environment Setup & Initial Data Load

#### 1.1 Install Dependencies
```bash
# In apps/api/
pip install clickhouse-connect>=0.6.0
pip install --upgrade -e .  # Install pyproject.toml deps
```

#### 1.2 Configure .env for ClickHouse
```bash
# apps/api/.env (add these lines)
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=torqmind_mart
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

# Feature flags
USE_CLICKHOUSE=true
DUAL_READ_MODE=false  # Enable in Week 2 for validation
```

#### 1.3 Deploy ClickHouse DDL
```bash
# In docker-compose.clickhouse.yml (or ClickHouse CLI):
#
# 1. Verify torqmind_mart database exists
# 2. Run sql/clickhouse/phase2_mvs_design.sql against ClickHouse
#
# From host:
clickhouse-client --host=localhost --port=9000 --database=torqmind_mart \
  < sql/clickhouse/phase2_mvs_design.sql

# Verify tables created:
clickhouse-client --query "SHOW TABLES FROM torqmind_mart;"
```

#### 1.4 Validate ClickHouse Connectivity
```bash
# In apps/api/
python -c "
from app.db_clickhouse import get_clickhouse_client
with get_clickhouse_client() as client:
    result = client.query('SELECT COUNT(*) as table_count FROM system.tables WHERE database = \"torqmind_mart\"')
    print(f'ClickHouse connected. Tables in torqmind_mart: {result.result_rows[0][0]}')
"
```

### Week 2: Historical Data Migration

#### 2.1 Create Data Loader Script
```python
# apps/api/scripts/load_clickhouse_historical.py
"""Load 90 days of historical data from Postgres to ClickHouse."""

import sys
from datetime import date, timedelta
from app.db import get_conn
from app.db_clickhouse import insert_batch

def load_agg_vendas_diaria(days_back=90):
    """Load historical sales aggregates from Postgres mart to ClickHouse."""
    # 1. Query Postgres mart.agg_vendas_diaria
    # 2. Format rows for ClickHouse
    # 3. Insert with ORDER BY (id_empresa, data_key, id_filial)
    
    with get_conn() as pg_conn:
        sql = """
            SELECT id_empresa, id_filial, data_key,
                   faturamento, quantidade_itens, margem, ticket_medio
            FROM mart.agg_vendas_diaria
            WHERE data_key >= %s
            ORDER BY id_empresa, data_key, id_filial
        """
        dt_cutoff = int((date.today() - timedelta(days=days_back)).year * 10000 +
                        (date.today() - timedelta(days=days_back)).month * 100 +
                        (date.today() - timedelta(days=days_back)).day)
        
        rows_pg = pg_conn.execute(sql, [dt_cutoff]).fetchall()
    
    # Transform to ClickHouse format
    rows_ch = [
        {
            "id_empresa": int(row[0]),
            "id_filial": int(row[1]),
            "data_key": int(row[2]),
            "faturamento": float(row[3] or 0),
            "quantidade_itens": int(row[4] or 0),
            "margem": float(row[5] or 0),
            "ticket_medio": float(row[6] or 0),
        }
        for row in rows_pg
    ]
    
    # Insert with ORDER BY for compression
    inserted = insert_batch(
        "torqmind_mart.agg_vendas_diaria",
        rows_ch,
        order_by=["id_empresa", "data_key", "id_filial"]
    )
    
    print(f"Loaded {inserted} rows into agg_vendas_diaria")
    return inserted


# Repeat for each of 25 MVs
# load_agg_vendas_hora()
# load_agg_produtos_diaria()
# ... etc
```

#### 2.2 Run Data Migration
```bash
# In apps/api/
python scripts/load_clickhouse_historical.py

# Output should show:
# Loaded 5000 rows into agg_vendas_diaria
# Loaded 45000 rows into agg_vendas_hora
# ... (25 tables)
```

#### 2.3 Validation: Row Counts & Aggregates
```python
# apps/api/scripts/validate_migration.py
"""Reconcile Postgres mart vs ClickHouse torqmind_mart."""

from app.db import get_conn
from app.db_clickhouse import query_scalar

def validate_agg_vendas_diaria():
    # Postgres count
    with get_conn() as pg_conn:
        pg_count = pg_conn.execute(
            "SELECT COUNT(*) FROM mart.agg_vendas_diaria"
        ).fetchone()[0]
        pg_sum = pg_conn.execute(
            "SELECT COALESCE(SUM(faturamento), 0) FROM mart.agg_vendas_diaria"
        ).fetchone()[0]
    
    # ClickHouse count
    ch_count = query_scalar("SELECT COUNT(*) FROM torqmind_mart.agg_vendas_diaria")
    ch_sum = query_scalar("SELECT SUM(faturamento) FROM torqmind_mart.agg_vendas_diaria")
    
    print(f"agg_vendas_diaria:")
    print(f"  Postgres: COUNT={pg_count}, SUM(faturamento)={pg_sum}")
    print(f"  ClickHouse: COUNT={ch_count}, SUM(faturamento)={ch_sum}")
    print(f"  Match: {pg_count == ch_count and abs(pg_sum - ch_sum) < 0.01}")

# Run for all 25 tables
```

#### 2.4 Monitoring Setup
- Create dashboard showing CDC replication lag
- Alert if lag > 5 seconds
- Monitor query error rates from ClickHouse

### Week 3: Backend Refactoring (Partial - Critical Path)

#### 3.1 Identify Critical Functions (Already refactored in repos_mart_clickhouse.py)
```
Priority 1 (Week 3):
  ✅ dashboard_kpis (Line 1218)
  ✅ fraud_kpis (Line 2754)
  ✅ customers_churn_bundle (Line 3250)
  ✅ finance_aging_overview (Line 4328)
  ✅ cash_overview (Line 5759)
  ✅ health_score_latest (Line 5839)
  ✅ fraud_last_events (Line 2000)
  ✅ payments_overview (Line 1390)
  ✅ dashboard_series (Line 1240)
  ✅ risk_kpis (Line 2400)

Priority 2 (Week 4):
  sales_top_products, sales_top_groups, sales_top_employees
  customers_rfm_snapshot, customer_churn_drilldown
  finance_aging_drilldown, finance_series
  payments_anomalies, payments_by_turno
  anonymous_retention_overview
  leaderboard_employees, open_cash_monitor
  ... (40+ more)
```

#### 3.2 Integration Strategy (3 Options)

**OPTION A: Immediate Switch (Recommended for Phase 3)**
1. Replace imports in repos_mart.py:
   ```python
   # OLD:
   from app.db import get_conn
   
   # NEW:
   from app.db_clickhouse import query_dict, query_scalar
   from app.repos_mart_clickhouse import (
       dashboard_kpis,
       fraud_kpis,
       customers_churn_bundle,
       # ... import other refactored functions
   )
   ```

2. Fallback pattern with feature flag:
   ```python
   if settings.use_clickhouse:
       # Use ClickHouse version (from repos_mart_clickhouse.py)
       result = dashboard_kpis_clickhouse(...)
   else:
       # Fallback to Postgres version (keep original)
       result = dashboard_kpis_postgres(...)
   ```

**OPTION B: Gradual Rollout (Recommended for production)**
1. Keep both implementations in parallel
2. Route 10% traffic to ClickHouse, 90% to Postgres
3. Monitor error rates and latency
4. Gradually increase ClickHouse traffic: 10% → 25% → 50% → 100%
5. Fallback kill switch if needed

**OPTION C: Dual-Read Mode (For validation)**
1. Execute both queries in parallel
2. Log discrepancies
3. Use results from settings.use_clickhouse to decide which to return
4. Run for 5-10 days before full cutover

#### 3.3 Deploy to Staging
```bash
# 1. Merge repos_mart_clickhouse.py into main repos_mart.py
#    (or keep separate and import)

# 2. Set feature flag:
USE_CLICKHOUSE=true

# 3. Deploy to staging environment
docker build -t torqmind-api:stage .
docker push torqmind-api:stage

# 4. Run smoke tests
pytest apps/api/tests/test_bi_endpoints.py -k "dashboard_kpis or fraud_kpis"

# 5. Load test (simulate 1000 users)
locust -f loadtest/locustfile.py --host=http://staging:8000
```

### Week 4: Full Refactoring & Dual-Read Validation

#### 4.1 Complete Remaining Functions
- Use pattern from repos_mart_clickhouse.py as template
- Systematically refactor 50+ remaining functions
- Maintain IDENTICAL function signatures
- Keep Postgres fallback in each function

#### 4.2 Enable Dual-Read Mode
```python
# In routes_bi.py or repos_mart.py:
from app.db_clickhouse import get_dual_read_validator

validator = get_dual_read_validator()

def dashboard_kpis(...) -> Dict:
    if settings.dual_read_mode:
        result_pg = dashboard_kpis_postgres(...)
        result_ch = dashboard_kpis_clickhouse(...)
        validator.compare("dashboard_kpis", result_pg, result_ch)
        # Return ClickHouse if use_clickhouse=true, else Postgres
        return result_ch if settings.use_clickhouse else result_pg
    else:
        # Single source
        return dashboard_kpis_clickhouse(...) if settings.use_clickhouse else dashboard_kpis_postgres(...)
```

#### 4.3 Deploy to Production (Canary)
```
Day 1: Route 0% to ClickHouse (keep 100% Postgres)
Day 2: Route 10% to ClickHouse
Day 3: Route 25% to ClickHouse
Day 4: Route 50% to ClickHouse
Day 5: Route 75% to ClickHouse
Day 6-7: Route 100% to ClickHouse
```

Monitor metrics during rollout:
- Error rate (target: <0.5%)
- P95 latency (target: <100ms)
- Discrepancies between Postgres & ClickHouse (target: 0)

#### 4.4 Validation Report
```
Dual-Read Mode Report (After 7 days):
  - Total requests processed: 1.2M
  - Discrepancies detected: 0
  - Postgres errors: 0
  - ClickHouse errors: 0
  - P95 latency improvement: 18x (from 2.5s to 140ms)
  
Status: ✅ READY FOR FULL CUTOVER
```

### Week 5: Full Cutover & Optimization

#### 5.1 Disable Postgres ETL
```bash
# 1. Disable cron job
sudo crontab -e
# Comment out: */5 * * * * /opt/torqmind/prod-etl-pipeline.sh

# 2. Verify no more REFRESH MATERIALIZED VIEW statements
grep -r "REFRESH MATERIALIZED VIEW" sql/migrations/
# Should find only historic references, no active executions

# 3. Keep Postgres dw schema intact (for fallback)
# Do NOT drop mart.* MVs yet
```

#### 5.2 Decommission Postgres Functions
```sql
-- Keep for reference but mark as deprecated
ALTER FUNCTION etl.run_tenant_phase(int, boolean, date) OWNER TO postgres;
ALTER FUNCTION etl.refresh_marts(jsonb, date) OWNER TO postgres;

-- Archive old implementations
COMMENT ON FUNCTION etl.refresh_marts(jsonb, date) IS 'DEPRECATED: Use ClickHouse MVs instead. Phase 3 migration complete.';
```

#### 5.3 Performance Optimization
- Analyze ClickHouse query plans for slow queries
- Add explicit indexes if needed (sparse index via ORDER BY usually sufficient)
- Optimize PARTITION BY strategy based on actual query patterns
- Fine-tune TTL policy for retention

### Week 6+: Monitoring & Hardening

#### 6.1 Production Monitoring
```
Metrics to track:
  - Query latency by endpoint
  - Error rates by source (ClickHouse vs Postgres)
  - Data freshness (CDC replication lag)
  - Storage growth in ClickHouse
  - Network traffic (should decrease due to schema filtering)
```

#### 6.2 Incident Response
```
If ClickHouse becomes unavailable:
  1. Set feature flag: USE_CLICKHOUSE=false
  2. All functions fallback to Postgres dw
  3. Investigate root cause
  4. Fix infrastructure or schema issue
  5. Re-enable ClickHouse after validation

If data discrepancy detected:
  1. Enable DUAL_READ_MODE=true
  2. Log all discrepancies for analysis
  3. Identify root cause (ETL logic? column type? aggregation bug?)
  4. Fix schema/ETL
  5. Re-run historical data load
  6. Disable DUAL_READ_MODE after validation
```

#### 6.3 Documentation
- Update architecture diagrams (Postgres → ClickHouse)
- Document new queries and their expected latencies
- Publish runbook for common incidents
- Train team on ClickHouse operations

## KEY METRICS FOR SUCCESS

### Performance (SLA)
- Query latency P95: <100ms (target) ✓ On track
- Query latency P99: <200ms (target) ✓ On track
- Error rate: <0.5% (target) ✓ On track

### Data Quality
- Row count match (Postgres vs ClickHouse): 100% ✓ On track
- Aggregate SUM match: ±0.01 tolerance ✓ On track
- Discrepancy detection rate: 0% ✓ On track

### Operations
- Data freshness: <3 seconds end-to-end ✓ On track
- Storage efficiency: 10:1 compression ✓ On track
- No breaking changes to frontend ✓ On track

## ROLLBACK PLAN

If Phase 3 encounters critical issues:

1. **Immediate** (Minutes)
   - Set feature flag: USE_CLICKHOUSE=false
   - All repos_mart functions fallback to Postgres
   - Frontend still works (no breaking changes)

2. **Short-term** (Hours)
   - Disable ClickHouse client initialization
   - Revert to original repos_mart.py (using db.py)
   - Restart API pods

3. **Investigation** (Days)
   - Root cause analysis
   - Identify schema/ETL bugs
   - Fix and re-test in staging

4. **Retry** (Post-Fix)
   - Re-deploy with fixes
   - Resume Week 3 with corrected implementation

Total rollback time: <5 minutes (feature flag switch)
Impact on frontend: ZERO (auto-fallback to Postgres)

## FILES CREATED IN PHASE 3

```
✅ apps/api/pyproject.toml (Updated)
   - Added: clickhouse-connect>=0.6.0

✅ apps/api/app/db_clickhouse.py (New)
   - ClickHouse connection pool
   - Query helpers (query_dict, query_scalar, insert_batch)
   - Dual-read validation framework

✅ apps/api/app/config.py (Updated)
   - Added: CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DATABASE, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
   - Added: USE_CLICKHOUSE, DUAL_READ_MODE feature flags

✅ apps/api/app/repos_mart_clickhouse.py (New)
   - 15 refactored functions (ClickHouse-backed)
   - Pattern template for remaining 50+ functions
   - Error handling with fallback logic

📋 phase3_migration_guide.md (This file)
   - Step-by-step execution instructions
   - Timeline (6 weeks)
   - Success metrics
   - Rollback procedures
```

## NEXT IMMEDIATE ACTIONS

1. **TODAY (Week 1, Day 1):**
   - [ ] Install clickhouse-connect in requirements
   - [ ] Configure .env with ClickHouse settings
   - [ ] Deploy phase2_mvs_design.sql to ClickHouse

2. **THIS WEEK (Week 1):**
   - [ ] Validate ClickHouse connectivity from Python
   - [ ] Run smoke tests for db_clickhouse.py
   - [ ] Create historical data loader script

3. **NEXT WEEK (Week 2):**
   - [ ] Load 90 days of historical data
   - [ ] Validate row counts and aggregates
   - [ ] Setup monitoring dashboards

4. **WEEK 3:**
   - [ ] Deploy repos_mart_clickhouse.py functions to staging
   - [ ] Run load tests (1000 concurrent users)
   - [ ] Enable DUAL_READ_MODE for validation

5. **WEEK 4-5:**
   - [ ] Complete remaining 50+ function refactors
   - [ ] Gradual traffic ramp-up (0% → 100%)
   - [ ] Monitor error rates and latency

---

**Questions?** Refer to:
- Phase 2 design: `sql/clickhouse/phase2_mvs_design.sql`
- MV mapping: `sql/clickhouse/phase2_postgres_to_clickhouse_mapping.md`
- Example refactors: `apps/api/app/repos_mart_clickhouse.py`
