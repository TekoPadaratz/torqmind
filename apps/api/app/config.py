"""TorqMind API settings.

PT-BR: Todas as configs são via variáveis de ambiente (.env) para rodar bem em Docker.
EN   : Everything is env-driven (.env) to keep Docker-friendly deployments.
"""

from datetime import date
import warnings

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Environment
    app_env: str = "dev"
    app_root_path: str = ""
    app_cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"
    app_cors_origin_regex: str = r"^https?://([a-zA-Z0-9.-]+|\d{1,3}(?:\.\d{1,3}){3})(:3000)?$"

    # Database (PostgreSQL)
    database_url: str | None = None
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_database: str = "TORQMIND"
    pg_user: str = "postgres"
    pg_password: str = "1234"
    db_pool_min_size: int = 2
    db_pool_max_size: int = 30
    db_pool_timeout_seconds: int = 30
    db_pool_max_idle_seconds: int = 300

    # Database (ClickHouse - Analytics)
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_database: str = "torqmind_mart"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""

    # Feature flags for Phase 3 migration
    use_clickhouse: bool = True  # When False, fallback to PostgreSQL dw
    dual_read_mode: bool = False  # When True, validate both sources
    refresh_legacy_pg_marts: bool = False  # Legacy PostgreSQL mart refresh is off in ClickHouse-first production

    # Business clock
    business_timezone: str = "America/Sao_Paulo"
    business_tenant_timezones: str = ""

    # Auth
    api_jwt_secret: str = "CHANGE_ME_SUPER_SECRET"
    api_jwt_issuer: str = "torqmind-api"
    api_access_token_minutes: int = 480
    platform_sovereign_emails: str = "teko94@gmail.com"

    # Ingestion
    # If True, /ingest requires X-Ingest-Key (recommended for production).
    ingest_require_key: bool = False
    ingest_batch_size: int = 5000
    ingest_retention_override_min_date: date | None = None
    ingest_retention_override_datasets: str = (
        "comprovantes,movprodutos,itensmovprodutos,formas_pgto_comprovantes,turnos"
    )

    # Telegram (optional)
    telegram_bot_token: str | None = None
    notify_min_severity: str = "CRITICAL"
    etl_internal_key: str | None = None

    # Jarvis AI (optional; deterministic fallback is always available)
    openai_api_key: str = ""
    jarvis_model_fast: str = "gpt-4.1-mini"
    jarvis_model_strong: str = "gpt-4.1"
    jarvis_ai_top_n: int = 10
    jarvis_ai_max_output_tokens: int = 500
    jarvis_ai_timeout_seconds: int = 30
    jarvis_ai_rpm_sleep_seconds: int = 2
    # Pricing is model-dependent and can change over time; keep env-configurable.
    jarvis_ai_input_cost_per_1m: float = 0.4
    jarvis_ai_output_cost_per_1m: float = 1.6

    # Micro risk (2-minute loop friendly)
    micro_risk_critical_min_score: int = 85
    micro_risk_critical_min_impact: float = 150.0

    class Config:
        env_file = ".env"
        extra = "ignore"


_BLOCKED_PATTERNS = (
    "change_me",
    "changeme",
    "default",
    "password",
    "postgres",
    "admin",
    "1234",
)

_PRODUCTIVE_ENVS = {"prod", "production", "homolog", "homologation", "staging"}


def _is_production_like_env(app_env: str | None) -> bool:
    return (app_env or "").strip().lower() in _PRODUCTIVE_ENVS


def _is_weak_secret(value: str | None, *, min_length: int | None = None) -> bool:
    """Return True for empty, placeholder, or trivially insecure values."""
    normalized = str(value or "").strip()
    if not normalized:
        return True
    if min_length is not None and len(normalized) < min_length:
        return True
    lowered = normalized.lower()
    return any(pattern in lowered for pattern in _BLOCKED_PATTERNS)


def _collect_security_violations(s: "Settings") -> list[str]:
    violations: list[str] = []

    if _is_weak_secret(s.api_jwt_secret, min_length=32):
        violations.append("API_JWT_SECRET must be strong and have at least 32 characters")

    if _is_weak_secret(s.pg_password):
        violations.append("POSTGRES_PASSWORD/PG_PASSWORD must be strong and cannot use placeholders")

    if str(s.clickhouse_user or "").strip().lower() == "default":
        violations.append("CLICKHOUSE_USER cannot be 'default' in production-like environments")

    if _is_weak_secret(s.clickhouse_password):
        violations.append("CLICKHOUSE_PASSWORD must be strong and cannot use placeholders")

    if not s.ingest_require_key:
        violations.append("INGEST_REQUIRE_KEY must be true in production-like environments")

    return violations


def _validate_production_settings(s: "Settings") -> None:
    """Fail fast in production-like envs and warn in dev/test/local."""
    violations = _collect_security_violations(s)
    if _is_production_like_env(s.app_env):
        if violations:
            raise SystemExit(
                f"FATAL: Refusing to start in {s.app_env} with insecure config:\n"
                + "\n".join(f"  - {violation}" for violation in violations)
            )
        return

    if violations:
        warnings.warn(
            "Non-production config is using permissive values for: " + ", ".join(violations),
            RuntimeWarning,
            stacklevel=2,
        )


settings = Settings()
_validate_production_settings(settings)
