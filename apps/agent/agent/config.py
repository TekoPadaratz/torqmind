from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import os
from typing import Any, Dict, Optional

import yaml


DEFAULT_DATASETS: Dict[str, Dict[str, Any]] = {
    "filiais": {"table": "dbo.FILIAIS", "watermark_column": "DATAREPL", "enabled": False},
    "funcionarios": {"table": "dbo.FUNCIONARIOS", "watermark_column": "DATAREPL", "enabled": False},
    "entidades": {"table": "dbo.ENTIDADES", "watermark_column": "DATAREPL", "enabled": False},
    "clientes": {"table": "dbo.ENTIDADES", "watermark_column": "DATAREPL", "enabled": False},
    "grupoprodutos": {"table": "dbo.GRUPOPRODUTOS", "watermark_column": "DATAREPL", "enabled": False},
    "localvendas": {"table": "dbo.LOCALVENDAS", "watermark_column": "DATAREPL", "enabled": False},
    "produtos": {"table": "dbo.PRODUTOS", "watermark_column": "DATAREPL", "enabled": False},
    "turnos": {"table": "dbo.TURNOS", "watermark_column": "DATAREPL", "enabled": False},
    "comprovantes": {"table": "dbo.COMPROVANTES", "watermark_column": "DATAREPL", "enabled": True},
    "movprodutos": {"table": "dbo.MOVPRODUTOS", "watermark_column": "DATAREPL", "enabled": True},
    "itensmovprodutos": {"table": "dbo.ITENSMOVPRODUTOS", "watermark_column": "DATAREPL", "enabled": True},
    "contaspagar": {"table": "dbo.CONTASPAGAR", "watermark_column": "DATAREPL", "enabled": False},
    "contasreceber": {"table": "dbo.CONTASRECEBER", "watermark_column": "DATAREPL", "enabled": False},
    "financeiro": {"table": "dbo.FINANCEIRO", "watermark_column": "DATAREPL", "enabled": False},
}


@dataclass
class SQLServerConfig:
    dsn: Optional[str] = None
    driver: str = "ODBC Driver 17 for SQL Server"
    server: str = ""
    database: str = ""
    user: str = ""
    password: str = ""
    encrypt: Optional[bool] = None
    trust_server_certificate: Optional[bool] = None
    login_timeout_seconds: int = 30


@dataclass
class APIConfig:
    base_url: str = "http://localhost:8000"
    ingest_key: Optional[str] = None
    empresa_id: Optional[int] = None


@dataclass
class RuntimeConfig:
    batch_size: int = 5000
    fetch_size: int = 2000
    max_retries: int = 5
    timeout_seconds: int = 30
    gzip_enabled: bool = True
    state_dir: str = "state"


@dataclass
class AppConfig:
    sqlserver: SQLServerConfig
    api: APIConfig
    runtime: RuntimeConfig
    datasets: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    id_empresa: Optional[int] = None
    id_db: Optional[int] = None


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _env_int(name: str, default: Optional[int]) -> Optional[int]:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _merge_dataset_configs(user_cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    merged = {k: dict(v) for k, v in DEFAULT_DATASETS.items()}
    for ds, cfg in (user_cfg or {}).items():
        key = ds.strip().lower()
        base = merged.get(key, {"enabled": False})
        base.update(cfg or {})
        merged[key] = base
    return merged


def _apply_env_overrides(raw: Dict[str, Any]) -> Dict[str, Any]:
    sql = raw.setdefault("sqlserver", {})
    api = raw.setdefault("api", {})
    runtime = raw

    # Legacy compatibility
    if "api_url" in raw and "base_url" not in api:
        api["base_url"] = raw["api_url"]

    sql["dsn"] = os.getenv("TORQMIND_SQLSERVER_DSN", sql.get("dsn"))
    sql["driver"] = os.getenv("TORQMIND_SQLSERVER_DRIVER", sql.get("driver", "ODBC Driver 17 for SQL Server"))
    sql["server"] = os.getenv("TORQMIND_SQLSERVER_SERVER", sql.get("server"))
    sql["database"] = os.getenv("TORQMIND_SQLSERVER_DATABASE", sql.get("database"))
    sql["user"] = os.getenv("TORQMIND_SQLSERVER_USER", sql.get("user"))
    sql["password"] = os.getenv("TORQMIND_SQLSERVER_PASSWORD", sql.get("password"))
    encrypt_env = os.getenv("TORQMIND_SQLSERVER_ENCRYPT")
    if encrypt_env is not None:
        sql["encrypt"] = encrypt_env.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    trust_env = os.getenv("TORQMIND_SQLSERVER_TRUST_SERVER_CERTIFICATE")
    if trust_env is not None:
        sql["trust_server_certificate"] = trust_env.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    sql["login_timeout_seconds"] = _env_int(
        "TORQMIND_SQLSERVER_LOGIN_TIMEOUT_SECONDS",
        int(sql.get("login_timeout_seconds", 30)),
    )

    api["base_url"] = os.getenv("TORQMIND_API_BASE_URL", api.get("base_url", "http://localhost:8000"))
    api["ingest_key"] = os.getenv("TORQMIND_INGEST_KEY", api.get("ingest_key"))
    empresa_env = os.getenv("TORQMIND_EMPRESA_ID")
    if empresa_env is not None:
        api["empresa_id"] = int(empresa_env)

    runtime["batch_size"] = _env_int("TORQMIND_BATCH_SIZE", int(runtime.get("batch_size", 5000)))
    runtime["fetch_size"] = _env_int("TORQMIND_FETCH_SIZE", int(runtime.get("fetch_size", 2000)))
    runtime["max_retries"] = _env_int("TORQMIND_MAX_RETRIES", int(runtime.get("max_retries", 5)))
    runtime["timeout_seconds"] = _env_int("TORQMIND_TIMEOUT_SECONDS", int(runtime.get("timeout_seconds", 30)))
    runtime["gzip_enabled"] = _env_bool("TORQMIND_GZIP_ENABLED", bool(runtime.get("gzip_enabled", True)))
    runtime["state_dir"] = os.getenv("TORQMIND_STATE_DIR", runtime.get("state_dir", "state"))

    raw["id_empresa"] = _env_int("TORQMIND_ID_EMPRESA", raw.get("id_empresa"))
    raw["id_db"] = _env_int("TORQMIND_ID_DB", raw.get("id_db"))

    enabled_env = os.getenv("TORQMIND_ENABLED_DATASETS")
    if enabled_env:
        enabled_set = {x.strip().lower() for x in enabled_env.split(",") if x.strip()}
        raw.setdefault("datasets", {})
        for ds in enabled_set:
            cur = raw["datasets"].get(ds, {})
            cur["enabled"] = True
            raw["datasets"][ds] = cur

    return raw


def load_config(config_path: str = "config.yaml") -> AppConfig:
    raw = _load_yaml(Path(config_path))
    raw = _apply_env_overrides(raw)

    sql = SQLServerConfig(**(raw.get("sqlserver") or {}))
    api = APIConfig(**(raw.get("api") or {}))
    runtime = RuntimeConfig(
        batch_size=int(raw.get("batch_size", raw.get("runtime", {}).get("batch_size", 5000))),
        fetch_size=int(raw.get("fetch_size", raw.get("runtime", {}).get("fetch_size", 2000))),
        max_retries=int(raw.get("max_retries", raw.get("runtime", {}).get("max_retries", 5))),
        timeout_seconds=int(raw.get("timeout_seconds", raw.get("runtime", {}).get("timeout_seconds", 30))),
        gzip_enabled=bool(raw.get("gzip_enabled", raw.get("runtime", {}).get("gzip_enabled", True))),
        state_dir=str(raw.get("state_dir", raw.get("runtime", {}).get("state_dir", "state"))),
    )

    datasets = _merge_dataset_configs(raw.get("datasets") or {})

    if raw.get("id_empresa") is not None and api.empresa_id is None:
        api.empresa_id = int(raw["id_empresa"])

    return AppConfig(
        sqlserver=sql,
        api=api,
        runtime=runtime,
        datasets=datasets,
        id_empresa=raw.get("id_empresa"),
        id_db=raw.get("id_db"),
    )
