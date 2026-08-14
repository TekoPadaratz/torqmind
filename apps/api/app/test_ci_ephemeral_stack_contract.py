from __future__ import annotations

import unittest
from pathlib import Path


def repo_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "docker-compose.ci.yml").is_file():
            return parent
    raise unittest.SkipTest("repository root is not available")


class CiEphemeralStackContractTest(unittest.TestCase):
    def test_ci_compose_is_disposable_and_has_no_initdb(self) -> None:
        source = (repo_root() / "docker-compose.ci.yml").read_text(encoding="utf-8")
        self.assertIn("name: torqmind-ci", source)
        self.assertIn("APP_ENV: test", source)
        self.assertIn("torqmind_ci", source)
        self.assertIn("sql/ci:/app/sql/ci:ro", source)
        self.assertNotIn("/docker-entrypoint-initdb.d", source)
        self.assertNotIn("172.30.0.8", source)
        self.assertNotIn("172.30.0.9", source)
        self.assertNotIn("172.30.0.10", source)
        self.assertNotIn("ports:", source)
        self.assertNotIn("--remove-orphans", source)

    def test_ci_env_targets_only_the_ephemeral_database(self) -> None:
        source = (repo_root() / ".env.ci").read_text(encoding="utf-8")
        assignments = "\n".join(
            line for line in source.splitlines() if line.strip() and not line.lstrip().startswith("#")
        )
        self.assertIn("APP_ENV=test", assignments)
        self.assertIn("PG_HOST=postgres", assignments)
        self.assertIn("PG_DATABASE=torqmind_ci", assignments)
        self.assertIn("COMPOSE_PROJECT_NAME=torqmind-ci", assignments)
        self.assertIn("@postgres:5432/torqmind_ci", assignments)
        self.assertNotIn("172.30.0.", assignments)
        self.assertNotIn("prod.app.env", assignments)
        self.assertNotIn("homolog", assignments.lower())

    def test_validate_workflow_uses_gha_postgres_service(self) -> None:
        source = (repo_root() / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        self.assertIn("postgres:16", source)
        self.assertIn("PG_HOST: 127.0.0.1", source)
        self.assertIn("PG_DATABASE: torqmind_ci", source)
        self.assertIn("APP_ENV: test", source)
        self.assertIn("TM_EPHEMERAL_LOCAL: \"1\"", source)
        self.assertIn("ci-assert-ephemeral-env.sh", source)
        self.assertIn("python -m app.cli.migrate", source)
        self.assertIn("app.test_ci_ephemeral_smoke", source)
        self.assertNotIn("172.30.0.8", source)
        self.assertNotIn("cp .env.e2e.local .env", source)
        self.assertNotIn("docker compose up -d --build\n", source)
        self.assertNotIn("--remove-orphans", source)

    def test_login_rate_limit_is_skipped_in_test_env(self) -> None:
        source = (repo_root() / "apps" / "api" / "app" / "main.py").read_text(encoding="utf-8")
        self.assertIn('str(settings.app_env or "").strip().lower() == "test"', source)
        self.assertIn('status_code=429', source)
