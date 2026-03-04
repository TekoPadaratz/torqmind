"""TorqMind API settings.

PT-BR: Todas as configs são via variáveis de ambiente (.env) para rodar bem em Docker.
EN   : Everything is env-driven (.env) to keep Docker-friendly deployments.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Environment
    app_env: str = "dev"

    # Database
    database_url: str | None = None
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_database: str = "TORQMIND"
    pg_user: str = "postgres"
    pg_password: str = "1234"

    # Auth
    api_jwt_secret: str = "CHANGE_ME_SUPER_SECRET"
    api_jwt_issuer: str = "torqmind-api"
    api_access_token_minutes: int = 60

    # Ingestion
    # If True, /ingest requires X-Ingest-Key (recommended for production).
    ingest_require_key: bool = False

    # Telegram (optional)
    telegram_bot_token: str | None = None
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


settings = Settings()
