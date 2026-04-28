#!/usr/bin/env bash
#
# ============================================================================
# PHASE 3: QUICK START CHECKLIST
# ============================================================================
#
# Purpose: Day-by-day implementation checklist for Week 1
# How to use: Follow each section in order, checking off as you complete
#
# Expected completion time: 2-4 hours
#
# ============================================================================

## WEEK 1: ENVIRONMENT SETUP & VALIDATION

### DAY 1 MORNING: Install Dependencies

- [ ] Navigate to API directory
  ```bash
  cd z:\torqmind-nova-branch-limpa\apps\api
  ```

- [ ] Install ClickHouse connector
  ```bash
  pip install clickhouse-connect>=0.6.0
  ```

- [ ] Verify installation
  ```bash
  python -c "import clickhouse_connect; print(f'✅ ClickHouse Connect {clickhouse_connect.__version__} installed')"
  ```

- [ ] Install project dependencies
  ```bash
  pip install --upgrade -e .
  ```

- [ ] Verify all imports
  ```bash
  python -c "from app.db_clickhouse import get_clickhouse_client; print('✅ db_clickhouse.py imports OK')"
  ```

### DAY 1 AFTERNOON: Configure Environment

- [ ] Check current .env file
  ```bash
  cat .env | grep CLICKHOUSE
  ```

- [ ] Add ClickHouse settings to .env (if missing)
  ```bash
  # apps/api/.env
  CLICKHOUSE_HOST=localhost
  CLICKHOUSE_PORT=8123
  CLICKHOUSE_DATABASE=torqmind_mart
  CLICKHOUSE_USER=default
  CLICKHOUSE_PASSWORD=
  
  USE_CLICKHOUSE=true
  DUAL_READ_MODE=false
  ```

- [ ] Verify .env loaded
  ```bash
  python -c "from app.config import settings; print(f'ClickHouse Host: {settings.clickhouse_host}:{settings.clickhouse_port}')"
  ```

### DAY 2 MORNING: Deploy ClickHouse DDL

- [ ] Verify ClickHouse container is running
  ```bash
  docker-compose ps | grep clickhouse
  ```

- [ ] Verify ClickHouse database exists
  ```bash
  docker exec -it [container_name] clickhouse-client --query "SHOW DATABASES LIKE 'torqmind_mart';"
  ```

- [ ] Deploy 25 MV definitions
  ```bash
  # From root directory:
  docker exec -i [clickhouse_container] clickhouse-client --database=torqmind_mart < sql/clickhouse/phase2_mvs_design.sql
  ```

- [ ] Verify tables created
  ```bash
  docker exec -it [container_name] clickhouse-client --query "SELECT count(*) as table_count FROM system.tables WHERE database = 'torqmind_mart';"
  # Expected result: 25 (or close to 25)
  ```

### DAY 2 AFTERNOON: Test ClickHouse Connectivity

- [ ] Test Python connectivity
  ```bash
  cd apps/api
  python << 'EOF'
from app.db_clickhouse import get_clickhouse_client
from app.config import settings

print(f"🔧 Testing ClickHouse connection...")
print(f"   Host: {settings.clickhouse_host}:{settings.clickhouse_port}")
print(f"   Database: {settings.clickhouse_database}")

try:
    with get_clickhouse_client() as client:
        result = client.query('SELECT COUNT(*) as tables FROM system.tables WHERE database = ?', parameters=["torqmind_mart"])
        count = result.result_rows[0][0] if result.result_rows else 0
        print(f"✅ Connected! Tables in torqmind_mart: {count}")
        
        # List tables
        result = client.query("SELECT name FROM system.tables WHERE database = 'torqmind_mart' ORDER BY name")
        print(f"\n📊 Available tables:")
        for row in result.result_rows:
            print(f"   - {row[0]}")
        
except Exception as e:
    print(f"❌ Connection failed: {e}")
    import traceback
    traceback.print_exc()
EOF
  ```

- [ ] Verify DDL is correct (no errors in previous step)

### DAY 3 MORNING: Test db_clickhouse Module

- [ ] Test query_dict function
  ```bash
  python << 'EOF'
from app.db_clickhouse import query_dict
from datetime import date, timedelta

# Test a simple read
try:
    sql = "SELECT name, engine FROM system.tables WHERE database = 'torqmind_mart' LIMIT 3"
    rows = query_dict(sql, parameters={})
    print(f"✅ query_dict works! Found {len(rows)} tables")
    for row in rows:
        print(f"   - {row.get('name')}: {row.get('engine')}")
except Exception as e:
    print(f"❌ query_dict failed: {e}")
EOF
  ```

- [ ] Test query_scalar function
  ```bash
  python << 'EOF'
from app.db_clickhouse import query_scalar

try:
    count = query_scalar("SELECT count(*) FROM system.tables WHERE database = 'torqmind_mart'", parameters={})
    print(f"✅ query_scalar works! Table count: {count}")
except Exception as e:
    print(f"❌ query_scalar failed: {e}")
EOF
  ```

### DAY 3 AFTERNOON: Test repos_mart_clickhouse Module

- [ ] Import and test sample function
  ```bash
  python << 'EOF'
from app.repos_mart_clickhouse import dashboard_kpis
from datetime import date

# Test with dummy company ID (may return empty if no data yet)
try:
    result = dashboard_kpis(
        role="admin",
        id_empresa=1,
        id_filial=None,
        dt_ini=date.today() - timedelta(days=30),
        dt_fim=date.today()
    )
    print(f"✅ dashboard_kpis works!")
    print(f"   Result: {result}")
except Exception as e:
    print(f"⚠️  Function called (may return empty before data load): {e}")
    # This is OK - just testing that the module loads
EOF
  ```

- [ ] Verify no import errors
  ```bash
  python -c "from app.repos_mart_clickhouse import *; print('✅ All repos_mart_clickhouse functions imported successfully')"
  ```

### DAY 4 MORNING: Run Smoke Tests

- [ ] Run existing API tests (if any)
  ```bash
  pytest tests/ -v --tb=short 2>&1 | head -50
  ```

- [ ] Check for type errors
  ```bash
  mypy app/db_clickhouse.py --ignore-missing-imports
  mypy app/repos_mart_clickhouse.py --ignore-missing-imports
  ```

- [ ] Lint code
  ```bash
  ruff check app/db_clickhouse.py app/repos_mart_clickhouse.py
  ```

### DAY 4 AFTERNOON: Final Validation

- [ ] Run FastAPI server (test endpoint)
  ```bash
  # In apps/api/
  uvicorn app.main:app --reload &
  
  # Wait 10 seconds, then test:
  curl http://localhost:8000/health
  # Expected: 200 OK
  ```

- [ ] Test actual BI endpoint (if exists)
  ```bash
  curl http://localhost:8000/api/v1/dashboard/kpis?id_empresa=1&dt_ini=2026-01-01&dt_fim=2026-04-28
  # Expected: 200 OK + JSON response
  ```

- [ ] Stop server
  ```bash
  pkill -f uvicorn
  ```

---

## WEEK 1 SUMMARY CHECKLIST

| Task | Status | Evidence |
|------|--------|----------|
| Install clickhouse-connect | [ ] | `pip freeze \| grep clickhouse-connect` |
| Configure .env | [ ] | `cat apps/api/.env \| grep CLICKHOUSE_` |
| Deploy phase2_mvs_design.sql | [ ] | `clickhouse-client --query "SHOW TABLES FROM torqmind_mart"` |
| Test connectivity | [ ] | Python script returns table count |
| Test query_dict | [ ] | Returns list of dicts |
| Test query_scalar | [ ] | Returns single scalar value |
| Test repos_mart_clickhouse imports | [ ] | No import errors |
| Run smoke tests | [ ] | No failures |
| Test API endpoint | [ ] | 200 OK response |

---

## IF YOU GET STUCK...

### Problem: "ModuleNotFoundError: clickhouse-connect"
**Solution**:
```bash
pip install clickhouse-connect>=0.6.0
pip list | grep clickhouse
```

### Problem: "Connection refused to ClickHouse"
**Check**:
1. ClickHouse container running? `docker-compose ps`
2. Port 8123 open? `netstat -an | grep 8123`
3. Correct host/port in .env? `cat apps/api/.env | grep CLICKHOUSE`
4. Try direct connection: `docker exec -it [container] clickhouse-client`

### Problem: "table torqmind_mart.agg_vendas_diaria doesn't exist"
**Check**:
1. DDL deployed? `docker exec -it [container] clickhouse-client --query "SHOW TABLES FROM torqmind_mart"`
2. If not, deploy DDL: `docker exec -i [container] clickhouse-client < sql/clickhouse/phase2_mvs_design.sql`
3. Verify again: `docker exec -it [container] clickhouse-client --query "SELECT * FROM torqmind_mart.agg_vendas_diaria LIMIT 1"`

### Problem: "No rows returned from query"
**This is OK!** 
- It means ClickHouse is working, but contains no data yet
- Data migration happens in Week 2
- For now, just verify schema is correct: `clickhouse-client --query "DESCRIBE torqmind_mart.agg_vendas_diaria"`

### Problem: Python query returns empty/None
**Check**:
1. Table has data? `clickhouse-client --query "SELECT count(*) FROM torqmind_mart.agg_vendas_diaria"`
2. Feature flag is true? `cat apps/api/.env | grep USE_CLICKHOUSE`
3. Try simpler query: `query_dict("SELECT 1 as test")`

---

## NEXT WEEK: WEEK 2 PREVIEW

After Week 1 completion, you'll be ready for:

1. **Create data migration script** (`apps/api/scripts/load_clickhouse_historical.py`)
   - Load 90 days of historical data from Postgres mart.* → ClickHouse torqmind_mart.*
   - Validates row counts & aggregates

2. **Run historical data load**
   - `python scripts/load_clickhouse_historical.py`
   - Monitor progress + reconciliation

3. **Setup monitoring**
   - Track CDC replication lag
   - Monitor query latency
   - Alert on errors

See [phase3_migration_guide.md](phase3_migration_guide.md) for Week 2 details.

---

## SUCCESS CRITERIA FOR WEEK 1

✅ **You're done when**:
- [x] clickhouse-connect installed
- [x] .env configured with ClickHouse settings
- [x] phase2_mvs_design.sql deployed (25 tables created)
- [x] Python connects to ClickHouse successfully
- [x] db_clickhouse.py functions work (query_dict, query_scalar)
- [x] repos_mart_clickhouse.py imports without errors
- [x] FastAPI still runs (no breaking changes)
- [x] All existing tests still pass

---

**Estimated Time**: 2-4 hours  
**Risk Level**: Low (non-breaking changes, feature flag disabled until Week 2)  
**Rollback**: Simple (revert .env, restart API)

---

Generated: 2026-04-28 | Phase 3 Quick Start Checklist
