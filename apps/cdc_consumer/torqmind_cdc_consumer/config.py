"""Configuration for TorqMind CDC Consumer."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Consumer configuration loaded from environment variables."""

    # Redpanda / Kafka
    redpanda_brokers: str = "redpanda:9092"
    cdc_consumer_group: str = "torqmind-cdc-consumer"
    cdc_topics: str = ""  # Comma-separated; empty = auto-subscribe via pattern
    cdc_topic_pattern: str = "^torqmind\\..*"
    cdc_batch_size: int = 500
    cdc_flush_interval_seconds: float = 5.0
    cdc_poll_timeout_seconds: float = 1.0
    cdc_auto_offset_reset: str = "earliest"

    # ClickHouse
    clickhouse_host: str = "clickhouse"
    clickhouse_port: int = 8123
    clickhouse_user: str = "torqmind"
    clickhouse_password: str = ""
    clickhouse_raw_db: str = "torqmind_raw"
    clickhouse_current_db: str = "torqmind_current"
    clickhouse_ops_db: str = "torqmind_ops"

    # Operational
    log_level: str = "INFO"
    max_errors_before_restart: int = 100
    enable_raw_writes: bool = True
    enable_current_writes: bool = True
    enable_ops_writes: bool = True

    model_config = {"env_prefix": "", "case_sensitive": False}


settings = Settings()
