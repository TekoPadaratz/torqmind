#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-/etc/torqmind/prod.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
ALLOW_INSECURE_ENV="${ALLOW_INSECURE_ENV:-0}"

ID_EMPRESA="${ID_EMPRESA:-1}"
ID_FILIAL="${ID_FILIAL:-}"
FROM_DATE="${FROM_DATE:-2025-01-01}"
TO_DATE="${TO_DATE:-}"

ASSUME_YES=0
DRY_RUN=0
SKIP_PURGE=0
SKIP_ETL=0
SKIP_VERIFY=0
INCLUDE_DIMENSIONS=0

# shellcheck source=deploy/scripts/lib/prod-env.sh
source "$ROOT_DIR/deploy/scripts/lib/prod-env.sh"

usage() {
  cat <<'EOF'
Usage:
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-rebuild-derived-from-stg.sh [flags]

Flags:
  --yes
  --dry-run
  --include-dimensions
  --skip-purge
  --skip-etl
  --skip-verify
  --id-empresa <id>
  --id-filial <id>
  --from-date <YYYY-MM-DD>
  --to-date <YYYY-MM-DD>
  --help

Examples:
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-rebuild-derived-from-stg.sh --yes --id-empresa 1 --from-date 2025-01-01
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-rebuild-derived-from-stg.sh --yes --include-dimensions --id-empresa 1 --from-date 2025-01-01
  ENV_FILE=/etc/torqmind/prod.env ./deploy/scripts/prod-rebuild-derived-from-stg.sh --dry-run --id-empresa 1 --id-filial 14458 --from-date 2025-01-01 --to-date 2025-03-31
EOF
}

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Comando obrigatorio ausente: $cmd" >&2
    exit 1
  fi
}

validate_iso_date() {
  local value="$1"
  python3 - "$value" <<'PY'
from datetime import date
import sys

date.fromisoformat(sys.argv[1])
PY
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --yes)
        ASSUME_YES=1
        ;;
      --dry-run)
        DRY_RUN=1
        ;;
      --include-dimensions)
        INCLUDE_DIMENSIONS=1
        ;;
      --skip-purge)
        SKIP_PURGE=1
        ;;
      --skip-etl)
        SKIP_ETL=1
        ;;
      --skip-verify)
        SKIP_VERIFY=1
        ;;
      --id-empresa)
        [[ $# -ge 2 ]] || { echo "--id-empresa requires a value" >&2; exit 2; }
        ID_EMPRESA="$2"
        shift
        ;;
      --id-filial)
        [[ $# -ge 2 ]] || { echo "--id-filial requires a value" >&2; exit 2; }
        ID_FILIAL="$2"
        shift
        ;;
      --from-date)
        [[ $# -ge 2 ]] || { echo "--from-date requires a value" >&2; exit 2; }
        FROM_DATE="$2"
        shift
        ;;
      --to-date)
        [[ $# -ge 2 ]] || { echo "--to-date requires a value" >&2; exit 2; }
        TO_DATE="$2"
        shift
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        echo "Unknown argument: $1" >&2
        usage >&2
        exit 2
        ;;
    esac
    shift
  done

  [[ "$ID_EMPRESA" =~ ^[0-9]+$ ]] || { echo "ID_EMPRESA must be numeric" >&2; exit 2; }
  if [[ -n "$ID_FILIAL" && ! "$ID_FILIAL" =~ ^[0-9]+$ ]]; then
    echo "ID_FILIAL must be numeric" >&2
    exit 2
  fi
  validate_iso_date "$FROM_DATE"
  if [[ -n "$TO_DATE" ]]; then
    validate_iso_date "$TO_DATE"
    if [[ "$TO_DATE" < "$FROM_DATE" ]]; then
      echo "TO_DATE must be greater than or equal to FROM_DATE" >&2
      exit 2
    fi
  fi
  if (( INCLUDE_DIMENSIONS )) && { [[ -n "$ID_FILIAL" ]] || [[ -n "$TO_DATE" ]]; }; then
    echo "--include-dimensions requires tenant-wide open-ended rebuild; omit --id-filial and --to-date" >&2
    exit 2
  fi
}

compose() {
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

pg() {
  local sql="$1"
  compose exec -T postgres sh -lc 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"' sh "$sql"
}

branch_filter() {
  if [[ -n "$ID_FILIAL" ]]; then
    printf ' AND id_filial = %s' "$ID_FILIAL"
  fi
}

scope_clause() {
  local date_expr="$1"
  local clause="id_empresa = ${ID_EMPRESA}"
  if [[ -n "$ID_FILIAL" ]]; then
    clause+=" AND id_filial = ${ID_FILIAL}"
  fi
  clause+=" AND (${date_expr}) >= DATE '${FROM_DATE}'"
  if [[ -n "$TO_DATE" ]]; then
    clause+=" AND (${date_expr}) <= DATE '${TO_DATE}'"
  fi
  printf '%s' "$clause"
}

confirm_destructive() {
  if (( DRY_RUN )) || (( ASSUME_YES )) || (( SKIP_PURGE )); then
    return 0
  fi

  local answer
  printf 'Purgar camadas derivadas para empresa=%s filial=%s intervalo=%s..%s? [y/N]: ' \
    "$ID_EMPRESA" "${ID_FILIAL:-todas}" "$FROM_DATE" "${TO_DATE:-aberto}" >/dev/tty
  read -r answer </dev/tty || exit 130
  case "${answer,,}" in
    y|yes|s|sim) ;;
    *)
      echo "Operacao cancelada" >&2
      exit 130
      ;;
  esac
}

coverage_gate() {
  local branch_sql
  branch_sql="$(branch_filter)"

  local comprovantes_row itens_row
  comprovantes_row="$(pg "SELECT count(*) || '|' || COALESCE(min(etl.business_date(etl.sales_event_timestamptz(payload, dt_evento)))::text, '') || '|' || COALESCE(max(etl.business_date(etl.sales_event_timestamptz(payload, dt_evento)))::text, '') FROM stg.comprovantes WHERE id_empresa = ${ID_EMPRESA}${branch_sql};")"
  itens_row="$(pg "SELECT count(*) || '|' || COALESCE(min(COALESCE(etl.business_date(dt_evento), DATE '1970-01-01'))::text, '') || '|' || COALESCE(max(COALESCE(etl.business_date(dt_evento), DATE '1970-01-01'))::text, '') FROM stg.itenscomprovantes WHERE id_empresa = ${ID_EMPRESA}${branch_sql};")"

  IFS='|' read -r comprovantes_count comprovantes_min comprovantes_max <<<"$comprovantes_row"
  IFS='|' read -r itens_count itens_min itens_max <<<"$itens_row"

  log "STG coverage stg.comprovantes count=${comprovantes_count:-0} min=${comprovantes_min:-n/a} max=${comprovantes_max:-n/a}"
  log "STG coverage stg.itenscomprovantes count=${itens_count:-0} min=${itens_min:-n/a} max=${itens_max:-n/a}"

  local gap=0
  if [[ -z "$comprovantes_min" || "$comprovantes_min" > "$FROM_DATE" ]]; then
    gap=1
    log "WARN STG comprovantes does not reach FROM_DATE=$FROM_DATE"
  fi
  if [[ -z "$itens_min" || "$itens_min" > "$FROM_DATE" ]]; then
    gap=1
    log "WARN STG itenscomprovantes does not reach FROM_DATE=$FROM_DATE"
  fi

  if (( gap == 0 )); then
    return 0
  fi
  if (( ASSUME_YES )); then
    echo "STG coverage does not reach FROM_DATE=$FROM_DATE. Rerun sem --yes para confirmar conscientemente ou ajuste FROM_DATE." >&2
    exit 1
  fi
  if (( DRY_RUN )); then
    return 0
  fi

  local answer
  printf 'STG nao cobre FROM_DATE=%s. Continuar mesmo assim? [y/N]: ' "$FROM_DATE" >/dev/tty
  read -r answer </dev/tty || exit 130
  case "${answer,,}" in
    y|yes|s|sim) ;;
    *)
      echo "Operacao cancelada por cobertura insuficiente da STG" >&2
      exit 130
      ;;
  esac
}

delete_scope() {
  local table_name="$1"
  local date_expr="$2"
  local clause
  clause="$(scope_clause "$date_expr")"
  if (( DRY_RUN )); then
    pg "SELECT '${table_name}|' || COUNT(*) FROM ${table_name} WHERE ${clause};"
    return 0
  fi
  pg "WITH deleted AS (DELETE FROM ${table_name} WHERE ${clause} RETURNING 1) SELECT '${table_name}|' || COUNT(*) FROM deleted;"
}

delete_dimension_scope() {
  local table_name="$1"
  local clause="id_empresa = ${ID_EMPRESA}"
  if (( DRY_RUN )); then
    pg "SELECT '${table_name}|' || COUNT(*) FROM ${table_name} WHERE ${clause};"
    return 0
  fi
  pg "WITH deleted AS (DELETE FROM ${table_name} WHERE ${clause} RETURNING 1) SELECT '${table_name}|' || COUNT(*) FROM deleted;"
}

purge_dimensions() {
  if (( ! INCLUDE_DIMENSIONS )); then
    return 0
  fi

  log "Purging safe reconstructible dimensions for tenant-wide rebuild"
  delete_dimension_scope "dw.dim_usuario_caixa"
  delete_dimension_scope "dw.dim_cliente"
  delete_dimension_scope "dw.dim_funcionario"
  delete_dimension_scope "dw.dim_produto"
  delete_dimension_scope "dw.dim_local_venda"
  delete_dimension_scope "dw.dim_grupo_produto"
  delete_dimension_scope "dw.dim_filial"
}

purge_scope() {
  if (( SKIP_PURGE )); then
    log "Skipping purge step"
    return 0
  fi

  log "Purging only safe derived layers"
  delete_scope "etl.pagamento_comprovante_bridge" "COALESCE(data_comp::date, data_conta, source_received_at::date)"
  delete_scope "dw.fact_pagamento_comprovante" "COALESCE(data_conta, dt_evento::date)"
  delete_scope "dw.fact_venda_item" "to_date(data_key::text, 'YYYYMMDD')"
  delete_scope "dw.fact_venda" "data::date"
  delete_scope "dw.fact_comprovante" "data::date"
  delete_scope "dw.fact_caixa_turno" "COALESCE(abertura_ts::date, fechamento_ts::date, CURRENT_DATE)"
  delete_scope "dw.fact_financeiro" "COALESCE(data_pagamento, vencimento, data_emissao)"
  purge_dimensions
}

run_etl() {
  if (( SKIP_ETL )); then
    log "Skipping ETL step"
    return 0
  fi

  local cmd=(env ENV_FILE="$ENV_FILE" COMPOSE_FILE="$COMPOSE_FILE" TRACK=full TENANT_ID="$ID_EMPRESA" FORCE_FULL=true FROM_DATE="$FROM_DATE")
  if [[ -n "$ID_FILIAL" ]]; then
    cmd+=(BRANCH_ID="$ID_FILIAL")
  fi
  if [[ -n "$TO_DATE" ]]; then
    cmd+=(TO_DATE="$TO_DATE")
  fi
  cmd+=("$ROOT_DIR/deploy/scripts/prod-etl-incremental.sh")

  if (( DRY_RUN )); then
    printf 'DRY-RUN etl:'
    printf ' %q' "${cmd[@]}"
    printf '\n'
    return 0
  fi

  "${cmd[@]}"
}

verify_scope() {
  if (( SKIP_VERIFY )); then
    log "Skipping verification step"
    return 0
  fi

  local branch_sql
  branch_sql="$(branch_filter)"

  log "Verification snapshot for STG and PostgreSQL DW"
  pg "SELECT 'stg.comprovantes|' || COUNT(*) || '|' || COALESCE(min(etl.business_date(etl.sales_event_timestamptz(payload, dt_evento)))::text, '') || '|' || COALESCE(max(etl.business_date(etl.sales_event_timestamptz(payload, dt_evento)))::text, '') FROM stg.comprovantes WHERE $(scope_clause "etl.business_date(etl.sales_event_timestamptz(payload, dt_evento))");"
  pg "SELECT 'stg.itenscomprovantes|' || COUNT(*) || '|' || COALESCE(min(COALESCE(etl.business_date(dt_evento), DATE '1970-01-01'))::text, '') || '|' || COALESCE(max(COALESCE(etl.business_date(dt_evento), DATE '1970-01-01'))::text, '') FROM stg.itenscomprovantes WHERE $(scope_clause "COALESCE(etl.business_date(dt_evento), DATE '1970-01-01')");"
  pg "SELECT 'dw.fact_comprovante|' || COUNT(*) || '|' || COALESCE(min(data::date)::text, '') || '|' || COALESCE(max(data::date)::text, '') FROM dw.fact_comprovante WHERE $(scope_clause "data::date");"
  pg "SELECT 'dw.fact_venda|' || COUNT(*) || '|' || COALESCE(min(data::date)::text, '') || '|' || COALESCE(max(data::date)::text, '') FROM dw.fact_venda WHERE $(scope_clause "data::date");"
  pg "SELECT 'dw.fact_venda_item|' || COUNT(*) || '|' || COALESCE(min(to_date(data_key::text, 'YYYYMMDD'))::text, '') || '|' || COALESCE(max(to_date(data_key::text, 'YYYYMMDD'))::text, '') FROM dw.fact_venda_item WHERE $(scope_clause "to_date(data_key::text, 'YYYYMMDD')");"
  pg "SELECT 'dw.fact_pagamento_comprovante|' || COUNT(*) || '|' || COALESCE(min(COALESCE(data_conta, dt_evento::date))::text, '') || '|' || COALESCE(max(COALESCE(data_conta, dt_evento::date))::text, '') FROM dw.fact_pagamento_comprovante WHERE $(scope_clause "COALESCE(data_conta, dt_evento::date)");"
  pg "SELECT 'etl.watermark|' || dataset || '|' || COALESCE(updated_at::text, '') FROM etl.watermark WHERE id_empresa = ${ID_EMPRESA} AND dataset IN ('comprovantes','formas_pgto_comprovantes','comprovantes_sales_fact','itenscomprovantes_sales_fact','turnos','financeiro','pagamento_comprovante_bridge') ORDER BY dataset;" || true
}

main() {
  parse_args "$@"
  require_cmd docker
  require_cmd python3
  tm_require_env_file "$ENV_FILE"
  if [[ "$ALLOW_INSECURE_ENV" != "1" ]]; then
    tm_require_prod_runtime_env "$ENV_FILE"
  fi

  cd "$ROOT_DIR"
  compose config --quiet >/dev/null
  compose ps >/dev/null

  log "Derived rebuild from STG"
  log "empresa=$ID_EMPRESA filial=${ID_FILIAL:-todas} from_date=$FROM_DATE to_date=${TO_DATE:-aberto} dry_run=$DRY_RUN include_dimensions=$INCLUDE_DIMENSIONS"

  coverage_gate
  confirm_destructive
  purge_scope
  run_etl
  verify_scope
}

main "$@"