#!/usr/bin/env bash

tm_require_env_file() {
  local env_file="$1"
  if [[ ! -f "$env_file" ]]; then
    echo "Arquivo de ambiente nao encontrado em $env_file" >&2
    return 1
  fi
}

tm_load_env_file() {
  local env_file="$1"
  tm_require_env_file "$env_file" || return 1
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
}

tm_is_blank() {
  [[ -z "${1//[[:space:]]/}" ]]
}

tm_is_placeholder() {
  local value="${1:-}"
  local upper="${value^^}"
  tm_is_blank "$value" && return 0
  [[ "$upper" == CHANGE_ME* || "$upper" == "<CHANGE_ME>" || "$upper" == "YOUR_VALUE_HERE" || "$upper" == "REPLACE_ME" ]]
}

tm_is_insecure_secret() {
  local value="${1:-}"
  tm_is_placeholder "$value" && return 0
  case "$value" in
    1234|TorqMind@123|@Crmjr105|CHANGE_ME|CHANGE_ME_API_JWT_SECRET|CHANGE_ME_POSTGRES_PASSWORD|CHANGE_ME_PLATFORM_MASTER_PASSWORD|CHANGE_ME_SEED_PASSWORD|CHANGE_ME_CHANNEL_BOOTSTRAP_PASSWORD)
      return 0
      ;;
  esac
  return 1
}

tm_require_safe_env() {
  local key="$1"
  local value="${!key:-}"
  if tm_is_insecure_secret "$value"; then
    echo "Variavel obrigatoria insegura ou ausente: $key" >&2
    return 1
  fi
}

tm_require_not_placeholder() {
  local key="$1"
  local value="${!key:-}"
  if tm_is_placeholder "$value"; then
    echo "Variavel obrigatoria ausente ou placeholder: $key" >&2
    return 1
  fi
}

tm_require_explicit_email() {
  local key="$1"
  local value="${!key:-}"
  if tm_is_placeholder "$value" || [[ "${value,,}" == "teko94@gmail.com" ]]; then
    echo "Variavel obrigatoria precisa ser explicita e nao pode usar default do repositorio: $key" >&2
    return 1
  fi
}

tm_warn_if_localhost_cors() {
  local origins="${APP_CORS_ORIGINS:-}"
  local regex="${APP_CORS_ORIGIN_REGEX:-}"
  if [[ "$origins" == *"http://localhost"* || "$origins" == *"http://127.0.0.1"* ]]; then
    echo "Aviso: APP_CORS_ORIGINS ainda contem localhost. Confirme se isso faz sentido no host de producao." >&2
  fi
  if [[ "$regex" == *"localhost"* ]]; then
    echo "Aviso: APP_CORS_ORIGIN_REGEX ainda aceita localhost. Revise se a API ficara exposta alem do nginx local." >&2
  fi
}

tm_require_prod_runtime_env() {
  local env_file="$1"
  tm_load_env_file "$env_file" || return 1
  tm_require_safe_env POSTGRES_PASSWORD || return 1
  tm_require_safe_env API_JWT_SECRET || return 1
  if [[ "${INGEST_REQUIRE_KEY:-true}" != "true" ]]; then
    echo "INGEST_REQUIRE_KEY precisa permanecer true em producao." >&2
    return 1
  fi
  tm_warn_if_localhost_cors
}

tm_require_prod_seed_env() {
  local env_file="$1"
  tm_require_prod_runtime_env "$env_file" || return 1
  tm_require_explicit_email PLATFORM_MASTER_EMAIL || return 1
  tm_require_not_placeholder CHANNEL_BOOTSTRAP_EMAIL || return 1
  tm_require_safe_env PLATFORM_MASTER_PASSWORD || return 1
  tm_require_safe_env SEED_PASSWORD || return 1
  if [[ -n "${CHANNEL_BOOTSTRAP_PASSWORD:-}" ]]; then
    tm_require_safe_env CHANNEL_BOOTSTRAP_PASSWORD || return 1
  fi
}
