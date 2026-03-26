from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import copy
import os
from typing import Any, Dict, Optional

import yaml

from agent.secrets import SecretStoreError, load_encrypted_json_file, save_encrypted_json_file


DEFAULT_DATASETS: Dict[str, Dict[str, Any]] = {
    "filiais": {"table": "dbo.FILIAIS", "watermark_column": "DATAREPL", "enabled": False},
    "funcionarios": {"table": "dbo.FUNCIONARIOS", "watermark_column": "DATAREPL", "enabled": False},
    "usuarios": {"table": "dbo.USUARIOS", "watermark_column": "DATAREPL", "enabled": True},
    "entidades": {"table": "dbo.ENTIDADES", "watermark_column": "DATAREPL", "enabled": False},
    "clientes": {"table": "dbo.ENTIDADES", "watermark_column": "DATAREPL", "enabled": False},
    "grupoprodutos": {"table": "dbo.GRUPOPRODUTOS", "watermark_column": "DATAREPL", "enabled": False},
    "localvendas": {"table": "dbo.LOCALVENDAS", "watermark_column": "DATAREPL", "enabled": False},
    "produtos": {"table": "dbo.PRODUTOS", "watermark_column": "DATAREPL", "enabled": False},
    "turnos": {
        "table": "dbo.TURNOS",
        "watermark_column": "DATAREPL",
        "revisit_open_clause": "COALESCE(ENCERRANTEFECHAMENTO, 0) = 0",
        "enabled": True,
    },
    "comprovantes": {"table": "dbo.COMPROVANTES", "watermark_column": "DATAREPL", "enabled": True},
    "movprodutos": {"table": "dbo.MOVPRODUTOS", "watermark_column": "DATAREPL", "enabled": True},
    "itensmovprodutos": {"table": "dbo.ITENSMOVPRODUTOS", "watermark_column": "DATAREPL", "enabled": True},
    "formas_pgto_comprovantes": {"table": "dbo.FORMAS_PGTO_COMPROVANTES", "watermark_column": "DATAREPL", "enabled": True},
    "movlctos": {"table": "dbo.MOVLCTOS", "watermark_column": "DATAREPL", "enabled": False},
    "contaspagar": {"table": "dbo.CONTASPAGAR", "watermark_column": "DATAREPL", "enabled": False},
    "contasreceber": {"table": "dbo.CONTASRECEBER", "watermark_column": "DATAREPL", "enabled": False},
    "financeiro": {"table": "dbo.FINANCEIRO", "watermark_column": "DATAREPL", "enabled": False},
}


@dataclass
class SQLServerConfig:
    dsn: Optional[str] = None
    driver: str = "ODBC Driver 17 for SQL Server"
    server: str = ""
    port: int = 1433
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
    spool_dir: str = "spool"
    spool_flush_max_files: int = 200
    interval_seconds: int = 60


@dataclass
class AppConfig:
    sqlserver: SQLServerConfig
    api: APIConfig
    runtime: RuntimeConfig
    datasets: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    id_empresa: Optional[int] = None
    id_db: Optional[int] = None


class AgentConfigError(RuntimeError):
    pass


def derive_encrypted_config_path(config_path: str | Path) -> Path:
    cfg_path = Path(config_path)
    if cfg_path.suffix.lower() == ".enc":
        return cfg_path
    return cfg_path.with_suffix(".enc")


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


def load_public_config(config_path: str | Path) -> Dict[str, Any]:
    return _load_yaml(Path(config_path))


def save_public_config(config_path: str | Path, raw: Dict[str, Any]) -> None:
    target = Path(config_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=False), encoding="utf-8")


def build_default_raw_config() -> Dict[str, Any]:
    return {
        "sqlserver": {
            "driver": "ODBC Driver 17 for SQL Server",
            "server": "",
            "port": 1433,
            "database": "",
            "user": "",
            "password": "",
            "encrypt": False,
            "trust_server_certificate": True,
            "login_timeout_seconds": 30,
        },
        "api": {
            "base_url": "http://localhost:8000",
            "ingest_key": "",
            "empresa_id": 1,
        },
        "runtime": {
            "batch_size": 5000,
            "fetch_size": 2000,
            "max_retries": 5,
            "timeout_seconds": 30,
            "gzip_enabled": True,
            "state_dir": "state",
            "spool_dir": "spool",
            "spool_flush_max_files": 200,
            "interval_seconds": 60,
        },
        "id_empresa": 1,
        "id_db": 1,
        "datasets": copy.deepcopy(DEFAULT_DATASETS),
    }


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
    runtime = raw.setdefault("runtime", {})

    if "api_url" in raw and "base_url" not in api:
        api["base_url"] = raw["api_url"]

    for legacy_key in (
        "batch_size",
        "fetch_size",
        "max_retries",
        "timeout_seconds",
        "gzip_enabled",
        "state_dir",
        "spool_dir",
        "spool_flush_max_files",
        "interval_seconds",
    ):
        if legacy_key in raw and legacy_key not in runtime:
            runtime[legacy_key] = raw[legacy_key]

    sql["dsn"] = os.getenv("TORQMIND_SQLSERVER_DSN", sql.get("dsn"))
    sql["driver"] = os.getenv("TORQMIND_SQLSERVER_DRIVER", sql.get("driver", "ODBC Driver 17 for SQL Server"))
    sql["server"] = os.getenv("TORQMIND_SQLSERVER_SERVER", sql.get("server"))
    sql["port"] = _env_int("TORQMIND_SQLSERVER_PORT", int(sql.get("port", 1433)))
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
    runtime["spool_dir"] = os.getenv("TORQMIND_SPOOL_DIR", runtime.get("spool_dir", "spool"))
    runtime["spool_flush_max_files"] = _env_int(
        "TORQMIND_SPOOL_FLUSH_MAX_FILES",
        int(runtime.get("spool_flush_max_files", 200)),
    )
    runtime["interval_seconds"] = _env_int(
        "TORQMIND_INTERVAL_SECONDS",
        int(runtime.get("interval_seconds", 60)),
    )

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


def _validate_required_fields(raw: Dict[str, Any], *, source_label: str) -> None:
    sql = raw.get("sqlserver") or {}
    api = raw.get("api") or {}
    runtime = raw.get("runtime") or {}
    missing: list[str] = []

    if not sql.get("dsn"):
        for key in ("server", "database", "user", "password"):
            if not sql.get(key):
                missing.append(f"sqlserver.{key}")
    if not api.get("base_url"):
        missing.append("api.base_url")
    if api.get("empresa_id") is None and not api.get("ingest_key"):
        missing.append("api.ingest_key")
    if not runtime.get("interval_seconds"):
        missing.append("runtime.interval_seconds")

    if missing:
        raise AgentConfigError(f"Missing required field(s) in {source_label}: " + ", ".join(sorted(missing)))


def load_raw_config(config_path: str | Path = "config.local.yaml") -> tuple[Dict[str, Any], Dict[str, Any]]:
    path = Path(config_path)
    if not path.exists():
        raise AgentConfigError(f"Configuration file not found: {path}")

    if path.suffix.lower() == ".enc":
        try:
            raw = load_encrypted_json_file(path)
        except SecretStoreError as exc:
            raise AgentConfigError(str(exc)) from exc
        if not raw:
            raise AgentConfigError("Encrypted config is empty.")
        return raw, {"config_path": str(path), "kind": "encrypted"}

    raw = load_public_config(path)
    if not raw:
        raise AgentConfigError(f"YAML config is empty: {path}")
    return raw, {"config_path": str(path), "kind": "yaml"}


def save_encrypted_config(config_path: str | Path, raw: Dict[str, Any]) -> None:
    normalized = _apply_env_overrides(copy.deepcopy(raw))
    _validate_required_fields(normalized, source_label=str(config_path))
    save_encrypted_json_file(config_path, raw)


def load_config(config_path: str = "config.local.yaml") -> AppConfig:
    raw, meta = load_raw_config(config_path)
    raw = _apply_env_overrides(copy.deepcopy(raw))
    _validate_required_fields(raw, source_label=meta["config_path"])

    sql = SQLServerConfig(**(raw.get("sqlserver") or {}))
    api = APIConfig(**(raw.get("api") or {}))
    runtime_raw = raw.get("runtime") or {}
    runtime = RuntimeConfig(
        batch_size=int(runtime_raw.get("batch_size", 5000)),
        fetch_size=int(runtime_raw.get("fetch_size", 2000)),
        max_retries=int(runtime_raw.get("max_retries", 5)),
        timeout_seconds=int(runtime_raw.get("timeout_seconds", 30)),
        gzip_enabled=bool(runtime_raw.get("gzip_enabled", True)),
        state_dir=str(runtime_raw.get("state_dir", "state")),
        spool_dir=str(runtime_raw.get("spool_dir", "spool")),
        spool_flush_max_files=int(runtime_raw.get("spool_flush_max_files", 200)),
        interval_seconds=int(runtime_raw.get("interval_seconds", 60)),
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


def migrate_yaml_to_encrypted_config(
    yaml_path: str | Path,
    encrypted_path: str | Path | None = None,
    *,
    delete_source: bool = True,
) -> Dict[str, Any]:
    raw = load_public_config(yaml_path)
    if not raw:
        raise AgentConfigError("No YAML configuration found to migrate.")

    target = Path(encrypted_path) if encrypted_path else derive_encrypted_config_path(yaml_path)
    save_encrypted_config(target, raw)
    if delete_source:
        Path(yaml_path).unlink(missing_ok=True)

    return {
        "source": str(yaml_path),
        "target": str(target),
    }
