"""TorqMind API settings.

PT-BR: Todas as configs são via variáveis de ambiente (.env) para rodar bem em Docker.
EN   : Everything is env-driven (.env) to keep Docker-friendly deployments.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
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

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
