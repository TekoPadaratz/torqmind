"""Runtime stack guard — impede API prod gravar em banco de homolog (e vice-versa).

Causa recorrente: `docker compose -f docker-compose.app.yml --env-file homolog.app.env`
recria `torqmind-api` apontando para `torqmind_homolog`. O agent recebe 200 e o
log parece saudável, mas a STG de produção congela.
"""

from __future__ import annotations

import logging
import os
import sys

logger = logging.getLogger(__name__)


def _env(name: str) -> str:
    return str(os.environ.get(name) or "").strip()


def _database_name_from_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    # postgresql://user:pass@host:5432/dbname?params
    without_query = raw.split("?", 1)[0]
    if "/" not in without_query:
        return ""
    return without_query.rsplit("/", 1)[-1].strip()


def assert_runtime_stack_or_exit() -> None:
    """Abort process boot if stack/env/database are inconsistent.

    Rules:
    - TORQMIND_STACK=prod (or container name torqmind-api without -homolog):
      APP_ENV must be prod-like; DATABASE_URL/PG_DATABASE must NOT contain homolog.
    - TORQMIND_STACK=homolog (or *-homolog container):
      must not target the production DB name `torqmind` (exact).
    """
    stack = _env("TORQMIND_STACK").lower()
    app_env = _env("APP_ENV").lower()
    hostname = _env("HOSTNAME").lower()  # docker often sets to container id; optional
    container = _env("TORQMIND_CONTAINER_NAME").lower() or hostname
    database_url = _env("DATABASE_URL")
    pg_database = _env("PG_DATABASE") or _env("POSTGRES_DB")
    db_from_url = _database_name_from_url(database_url)
    db_name = (pg_database or db_from_url).lower()

    # Infer stack from explicit marker or container naming convention.
    if not stack:
        if "homolog" in container or container.endswith("-homolog"):
            stack = "homolog"
        elif container in {"torqmind-api", "api"} or _env("TORQMIND_FORCE_PROD_GUARD").lower() in {
            "1",
            "true",
            "yes",
        }:
            stack = "prod"

    # Tests / local: no stack pin → no hard fail.
    if not stack or stack in {"dev", "local", "test"}:
        return

    errors: list[str] = []

    if stack == "prod":
        if app_env in {"homolog", "staging"}:
            errors.append(f"APP_ENV={app_env!r} is not allowed on TORQMIND_STACK=prod")
        if "homolog" in db_name:
            errors.append(
                f"database={db_name!r} looks like homolog; prod API must use production DB"
            )
        if "homolog" in database_url.lower():
            errors.append("DATABASE_URL contains 'homolog' on TORQMIND_STACK=prod")
    elif stack == "homolog":
        if db_name == "torqmind":
            errors.append(
                "homolog API refused to start against production database name 'torqmind'"
            )
        if app_env in {"prod", "production"}:
            errors.append(f"APP_ENV={app_env!r} is not allowed on TORQMIND_STACK=homolog")

    if not errors:
        logger.info(
            "runtime_guard ok stack=%s app_env=%s database=%s",
            stack,
            app_env or "?",
            db_name or "?",
        )
        return

    msg = (
        "FATAL runtime_guard: API boot aborted to protect production/homolog isolation. "
        + " | ".join(errors)
        + ". Use docker-compose.app.yml + /etc/torqmind/prod.app.env for prod, "
        "or docker-compose.homolog.yml -p torqmind-homolog + homolog.app.env for homolog."
    )
    logger.error(msg)
    print(msg, file=sys.stderr)
    raise SystemExit(2)
