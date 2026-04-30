#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ALLOW_INSECURE_ENV="${ALLOW_INSECURE_ENV:-0}"
ID_EMPRESA="${ID_EMPRESA:?missing ID_EMPRESA}"
ID_FILIAL="${ID_FILIAL:-}"

# shellcheck source=deploy/scripts/lib/prod-env.sh
source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

if [[ "$ALLOW_INSECURE_ENV" != "1" ]]; then
  tm_require_prod_runtime_env "$ENV_FILE"
fi

cd "$ROOT_DIR"

if ! [[ "$ID_EMPRESA" =~ ^[0-9]+$ ]]; then
  echo "ID_EMPRESA must be numeric" >&2
  exit 2
fi
if [[ -n "$ID_FILIAL" && "$ID_FILIAL" != "-1" && ! "$ID_FILIAL" =~ ^[0-9]+$ ]]; then
  echo "ID_FILIAL must be numeric or -1" >&2
  exit 2
fi

compose() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

container_env() {
  local service="$1"
  local key="$2"
  compose exec -T "$service" sh -lc "printf '%s' \"\${${key}:-}\""
}

pg() {
  local sql="$1"
  compose exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"' sh "$sql"
}

CLICKHOUSE_USER="${CLICKHOUSE_USER:-$(container_env clickhouse CLICKHOUSE_USER)}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-$(container_env clickhouse CLICKHOUSE_PASSWORD)}"

clickhouse_client_args=(clickhouse-client)
if [[ -n "${CLICKHOUSE_USER:-}" ]]; then
  clickhouse_client_args+=(--user "$CLICKHOUSE_USER")
fi
if [[ -n "${CLICKHOUSE_PASSWORD:-}" ]]; then
  clickhouse_client_args+=(--password "$CLICKHOUSE_PASSWORD")
fi

ch() {
  compose exec -T clickhouse "${clickhouse_client_args[@]}" "$@"
}

errors=0
warnings=0

error() {
  echo "ERROR: $*" >&2
  errors=$((errors + 1))
}

warn() {
  echo "WARN: $*" >&2
  warnings=$((warnings + 1))
}

ok() {
  echo "OK: $*"
}

ch_branch=""
pg_branch=""
scope_label="empresa=${ID_EMPRESA}"
if [[ -n "$ID_FILIAL" && "$ID_FILIAL" != "-1" ]]; then
  ch_branch=" AND id_filial = ${ID_FILIAL}"
  pg_branch=" AND id_filial = ${ID_FILIAL}"
  scope_label="${scope_label} filial=${ID_FILIAL}"
else
  scope_label="${scope_label} filial=todas"
fi

table_exists() {
  local database="$1"
  local table="$2"
  ch --query "SELECT count() FROM system.tables WHERE database = '${database}' AND name = '${table}'" || true
}

echo "== TorqMind semantic mart audit (${scope_label}) =="
echo

required_ch_tables=(
  "torqmind_dw.dim_forma_pagamento"
  "torqmind_dw.dim_filial"
  "torqmind_dw.dim_usuario_caixa"
  "torqmind_mart.agg_pagamentos_diaria"
  "torqmind_mart.agg_pagamentos_turno"
  "torqmind_mart.agg_caixa_forma_pagamento"
  "torqmind_mart.fraude_cancelamentos_eventos"
  "torqmind_mart.risco_turno_local_diaria"
  "torqmind_mart.finance_aging_daily"
)

for qualified in "${required_ch_tables[@]}"; do
  db="${qualified%%.*}"
  table="${qualified#*.}"
  if [[ "$(table_exists "$db" "$table")" != "1" ]]; then
    error "required table ${qualified} is missing"
  else
    ok "${qualified} exists"
  fi
done

if [[ "$errors" -gt 0 ]]; then
  echo
  echo "Semantic audit stopped before data checks because required objects are missing."
  exit 1
fi

echo
echo "payment labels:"
mapping_count="$(ch --query "SELECT count() FROM torqmind_dw.dim_forma_pagamento WHERE active")"
if [[ "$mapping_count" -le 0 ]]; then
  error "torqmind_dw.dim_forma_pagamento has no active payment labels"
else
  ok "active payment labels=${mapping_count}"
fi

daily_forma="$(ch --query "SELECT count() FROM torqmind_mart.agg_pagamentos_diaria WHERE id_empresa = ${ID_EMPRESA}${ch_branch} AND startsWith(label, 'FORMA_')")"
turno_forma="$(ch --query "SELECT count() FROM torqmind_mart.agg_pagamentos_turno WHERE id_empresa = ${ID_EMPRESA}${ch_branch} AND startsWith(label, 'FORMA_')")"
cash_forma="$(ch --query "SELECT count() FROM torqmind_mart.agg_caixa_forma_pagamento WHERE id_empresa = ${ID_EMPRESA}${ch_branch} AND startsWith(forma_label, 'FORMA_')")"
echo "agg_pagamentos_diaria FORMA_* rows=${daily_forma}"
echo "agg_pagamentos_turno FORMA_* rows=${turno_forma}"
echo "agg_caixa_forma_pagamento FORMA_* rows=${cash_forma}"
if [[ "$daily_forma" != "0" || "$turno_forma" != "0" || "$cash_forma" != "0" ]]; then
  error "payment marts still expose FORMA_* labels"
else
  ok "payment marts expose human payment labels"
fi

echo
echo "human dimensions:"
fraud_filial_missing="$(ch --query "SELECT count() FROM torqmind_mart.fraude_cancelamentos_eventos e INNER JOIN torqmind_dw.dim_filial f ON f.id_empresa = e.id_empresa AND f.id_filial = e.id_filial WHERE e.id_empresa = ${ID_EMPRESA}${ch_branch} AND e.filial_nome = '' AND f.nome != ''")"
fraud_usuario_missing="$(ch --query "SELECT count() FROM torqmind_mart.fraude_cancelamentos_eventos e INNER JOIN torqmind_dw.dim_usuario_caixa u ON u.id_empresa = e.id_empresa AND u.id_filial = e.id_filial AND u.id_usuario = e.id_usuario WHERE e.id_empresa = ${ID_EMPRESA}${ch_branch} AND e.usuario_nome = '' AND u.nome != ''")"
risk_turn_filial_missing="$(ch --query "SELECT count() FROM torqmind_mart.risco_turno_local_diaria r INNER JOIN torqmind_dw.dim_filial f ON f.id_empresa = r.id_empresa AND f.id_filial = r.id_filial WHERE r.id_empresa = ${ID_EMPRESA}${ch_branch} AND r.filial_nome = '' AND f.nome != ''")"
echo "fraude_cancelamentos_eventos filial missing despite dim=${fraud_filial_missing}"
echo "fraude_cancelamentos_eventos usuario missing despite dim=${fraud_usuario_missing}"
echo "risco_turno_local_diaria filial missing despite dim=${risk_turn_filial_missing}"
if [[ "$fraud_filial_missing" != "0" || "$fraud_usuario_missing" != "0" || "$risk_turn_filial_missing" != "0" ]]; then
  error "semantic marts lost filial/operator labels that exist in dimensions"
else
  ok "fraud/risk marts preserve available filial/operator labels"
fi

echo
echo "finance dates:"
finance_epoch_rows="$(ch --query "SELECT count() FROM torqmind_mart.finance_aging_daily WHERE id_empresa = ${ID_EMPRESA}${ch_branch} AND dt_ref <= toDate('1971-01-01')")"
finance_fact_rows="$(ch --query "SELECT count() FROM torqmind_dw.fact_financeiro WHERE id_empresa = ${ID_EMPRESA}${ch_branch}")"
finance_mart_rows="$(ch --query "SELECT count() FROM torqmind_mart.finance_aging_daily WHERE id_empresa = ${ID_EMPRESA}${ch_branch}")"
echo "finance_aging_daily epoch rows=${finance_epoch_rows}"
echo "finance facts=${finance_fact_rows} finance mart rows=${finance_mart_rows}"
if [[ "$finance_epoch_rows" != "0" ]]; then
  error "finance mart contains invalid epoch-like dt_ref"
else
  ok "finance mart has no 1970-like dates"
fi
if [[ "$finance_fact_rows" -gt 0 && "$finance_mart_rows" -eq 0 ]]; then
  error "financial facts exist but finance_aging_daily is empty"
fi

echo
echo "competitor prices:"
competitor_table_exists="$(pg "SELECT to_regclass('app.competitor_fuel_prices') IS NOT NULL;")"
if [[ "$competitor_table_exists" == "t" ]]; then
  competitor_count="$(pg "SELECT count(*)::bigint FROM app.competitor_fuel_prices WHERE id_empresa = ${ID_EMPRESA}${pg_branch};")"
  ok "app.competitor_fuel_prices rows=${competitor_count}"
else
  warn "app.competitor_fuel_prices table is not present"
fi

echo
echo "Semantic audit summary: errors=${errors} warnings=${warnings}"
if [[ "$errors" -gt 0 ]]; then
  exit 1
fi
exit 0
