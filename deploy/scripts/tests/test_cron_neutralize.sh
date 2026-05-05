#!/usr/bin/env bash
set -Eeuo pipefail
# Unit tests for _comment_legacy_cron_lines function
# Run: bash deploy/scripts/tests/test_cron_neutralize.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

PASS=0
FAIL=0

assert_eq() {
  local label="$1" expected="$2" actual="$3"
  if [[ "$expected" == "$actual" ]]; then
    printf '  PASS: %s\n' "$label"
    PASS=$((PASS + 1))
  else
    printf '  FAIL: %s\n    expected: %s\n    actual:   %s\n' "$label" "$expected" "$actual"
    FAIL=$((FAIL + 1))
  fi
}

# Source only the function we need (mock the rest)
LEGACY_ETL_PATTERNS=(
  "prod-etl-pipeline.sh"
  "prod-etl-incremental.sh"
  "prod-homologation-apply.sh"
  "prod-rebuild-derived-from-stg.sh"
  "etl_incremental"
  "etl_orchestrator"
)

# Extract the function from the script
eval "$(sed -n '/_comment_legacy_cron_lines()/,/^}/p' "$ROOT_DIR/deploy/scripts/prod-realtime-cutover-apply.sh")"

TS="20260501_120000"

echo "=== Test 1: Empty crontab ==="
input=""
output="$(printf '%s' "$input" | _comment_legacy_cron_lines "$TS" "${LEGACY_ETL_PATTERNS[@]}")"
assert_eq "empty input produces empty output" "" "$output"

echo ""
echo "=== Test 2: No legacy entries ==="
input="0 * * * * /usr/bin/health-check.sh
30 6 * * * /opt/torqmind/reconcile.sh"
expected="$input"
output="$(printf '%s\n' "$input" | _comment_legacy_cron_lines "$TS" "${LEGACY_ETL_PATTERNS[@]}")"
assert_eq "non-legacy lines unchanged" "$expected" "$output"

echo ""
echo "=== Test 3: Active prod-etl-pipeline.sh ==="
input="0 */2 * * * /opt/torqmind/deploy/scripts/prod-etl-pipeline.sh --id-empresa 1
30 6 * * * /opt/torqmind/reconcile.sh"
expected="# TORQMIND_LEGACY_ETL_DISABLED_${TS} 0 */2 * * * /opt/torqmind/deploy/scripts/prod-etl-pipeline.sh --id-empresa 1
30 6 * * * /opt/torqmind/reconcile.sh"
output="$(printf '%s\n' "$input" | _comment_legacy_cron_lines "$TS" "${LEGACY_ETL_PATTERNS[@]}")"
assert_eq "prod-etl-pipeline.sh is commented" "$expected" "$output"

echo ""
echo "=== Test 4: Already commented line is NOT double-commented ==="
input="# TORQMIND_LEGACY_ETL_DISABLED_20260401 0 */2 * * * /opt/torqmind/deploy/scripts/prod-etl-pipeline.sh
30 6 * * * /opt/torqmind/reconcile.sh"
expected="$input"
output="$(printf '%s\n' "$input" | _comment_legacy_cron_lines "$TS" "${LEGACY_ETL_PATTERNS[@]}")"
assert_eq "already commented line left alone" "$expected" "$output"

echo ""
echo "=== Test 5: Path with many slashes ==="
input="0 3 * * * /home/deploy/projects/TorqMind/deploy/scripts/prod-rebuild-derived-from-stg.sh --from-date 2025-01-01 --id-empresa 1"
expected="# TORQMIND_LEGACY_ETL_DISABLED_${TS} 0 3 * * * /home/deploy/projects/TorqMind/deploy/scripts/prod-rebuild-derived-from-stg.sh --from-date 2025-01-01 --id-empresa 1"
output="$(printf '%s\n' "$input" | _comment_legacy_cron_lines "$TS" "${LEGACY_ETL_PATTERNS[@]}")"
assert_eq "path with slashes handled correctly" "$expected" "$output"

echo ""
echo "=== Test 6: Multiple legacy entries ==="
input="0 */2 * * * /opt/scripts/prod-etl-pipeline.sh
15 * * * * /opt/scripts/prod-etl-incremental.sh
30 6 * * * /opt/scripts/reconcile.sh
0 4 * * 0 /opt/scripts/etl_orchestrator.py"
expected="# TORQMIND_LEGACY_ETL_DISABLED_${TS} 0 */2 * * * /opt/scripts/prod-etl-pipeline.sh
# TORQMIND_LEGACY_ETL_DISABLED_${TS} 15 * * * * /opt/scripts/prod-etl-incremental.sh
30 6 * * * /opt/scripts/reconcile.sh
# TORQMIND_LEGACY_ETL_DISABLED_${TS} 0 4 * * 0 /opt/scripts/etl_orchestrator.py"
output="$(printf '%s\n' "$input" | _comment_legacy_cron_lines "$TS" "${LEGACY_ETL_PATTERNS[@]}")"
assert_eq "multiple legacy entries all commented" "$expected" "$output"

echo ""
echo "=== Test 7: Indented comment preserved ==="
input="  # already a comment with prod-etl-pipeline.sh
0 */2 * * * /opt/scripts/prod-etl-pipeline.sh"
expected="  # already a comment with prod-etl-pipeline.sh
# TORQMIND_LEGACY_ETL_DISABLED_${TS} 0 */2 * * * /opt/scripts/prod-etl-pipeline.sh"
output="$(printf '%s\n' "$input" | _comment_legacy_cron_lines "$TS" "${LEGACY_ETL_PATTERNS[@]}")"
assert_eq "indented comment not touched, active line commented" "$expected" "$output"

echo ""
echo "============================================"
echo "Results: PASS=$PASS FAIL=$FAIL"
if (( FAIL > 0 )); then
  echo "FAILED"
  exit 1
fi
echo "ALL TESTS PASSED"
