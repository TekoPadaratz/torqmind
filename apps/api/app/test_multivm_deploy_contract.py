from __future__ import annotations

import unittest
from pathlib import Path

import yaml


def repo_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "docker-compose.app.yml").is_file():
            return parent
    raise unittest.SkipTest("repository root is not available")


def read(path: str) -> str:
    return (repo_root() / path).read_text(encoding="utf-8")


def compose(path: str) -> dict:
    return yaml.safe_load(read(path))


class MultiVmDeployContractTest(unittest.TestCase):
    def test_app_compose_has_no_local_postgres_or_clickhouse(self) -> None:
        data = compose("docker-compose.app.yml")
        services = data["services"]
        self.assertEqual(set(services), {"api", "web", "nginx"})
        self.assertNotIn("postgres", services)
        self.assertNotIn("clickhouse", services)
        self.assertNotIn("depends_on", services["api"])

    def test_app_compose_uses_remote_pg_and_clickhouse_env(self) -> None:
        api_env = compose("docker-compose.app.yml")["services"]["api"]["environment"]
        self.assertIn("PG_HOST", api_env)
        self.assertIn("CLICKHOUSE_HOST", api_env)
        self.assertIn("DATABASE_URL", api_env)
        self.assertNotEqual(api_env["PG_HOST"], "postgres")
        self.assertNotEqual(api_env["CLICKHOUSE_HOST"], "clickhouse")
        self.assertIn("CHANGE_ME_PRIVATE_POSTGRES_HOST", api_env["PG_HOST"])
        self.assertIn("CHANGE_ME_PRIVATE_ANALYTICS_HOST", api_env["CLICKHOUSE_HOST"])
        self.assertIn("CHANGE_ME_PRIVATE_POSTGRES_HOST", api_env["DATABASE_URL"])

    def test_pg_compose_only_runs_postgres_with_logical_replication(self) -> None:
        data = compose("docker-compose.pg.yml")
        self.assertEqual(set(data["services"]), {"postgres"})
        pg = data["services"]["postgres"]
        command_text = "\n".join(pg["command"])
        self.assertIn("wal_level=logical", command_text)
        self.assertIn("max_replication_slots=", command_text)
        self.assertIn("max_wal_senders=", command_text)
        self.assertIn("${PG_BIND_IP:-127.0.0.1}:${PG_PORT:-5432}:5432", pg["ports"])
        self.assertIn("pgdata_prod", data["volumes"])

    def test_analytics_compose_contains_streaming_services(self) -> None:
        services = compose("docker-compose.analytics.yml")["services"]
        self.assertEqual(set(services), {"clickhouse", "redpanda", "debezium-connect", "cdc-consumer"})
        self.assertIn("POSTGRES_HOST", services["debezium-connect"]["environment"])
        cdc_env = services["cdc-consumer"]["environment"]
        self.assertEqual(cdc_env["CLICKHOUSE_HOST"], "${CDC_CLICKHOUSE_HOST:-clickhouse}")
        self.assertIn("chdata_prod", compose("docker-compose.analytics.yml")["volumes"])
        self.assertIn("redpanda_data", compose("docker-compose.analytics.yml")["volumes"])

    def test_cluster_env_example_contains_required_variables(self) -> None:
        source = read("deploy/env/cluster.env.example")
        for key in (
            "TORQMIND_SSH_USER",
            "TORQMIND_PG_HOST",
            "TORQMIND_ANALYTICS_HOST",
            "TORQMIND_APP_HOST",
            "TORQMIND_PG_PRIVATE_IP",
            "TORQMIND_ANALYTICS_PRIVATE_IP",
            "TORQMIND_APP_PRIVATE_IP",
            "TORQMIND_REPO_DIR",
            "TORQMIND_BRANCH",
            "TORQMIND_ENV_DIR",
        ):
            self.assertIn(f"{key}=", source)

    def test_multivm_scripts_support_dry_run_and_validate_ssh(self) -> None:
        script_dir = repo_root() / "deploy" / "scripts"
        scripts = sorted(script_dir.glob("prod-multivm-*.sh"))
        self.assertGreaterEqual(len(scripts), 8)
        for script in scripts:
            source = script.read_text(encoding="utf-8")
            self.assertIn("--dry-run", source, script.name)
        bootstrap = read("deploy/scripts/prod-multivm-bootstrap.sh")
        self.assertIn("tm_mv_validate_ssh_all", bootstrap)
        lib = read("deploy/scripts/lib/multivm.sh")
        self.assertIn("-o BatchMode=yes", lib)

    def test_install_cron_uses_flock_and_does_not_duplicate_line(self) -> None:
        source = read("deploy/scripts/prod-multivm-install-cron.sh")
        self.assertIn("flock -n", source)
        self.assertIn("prod-etl-incremental-cron.log", source)
        self.assertIn("grep -v 'TorqMind multi-VM incremental ETL'", source)
        self.assertIn("grep -v 'prod-etl-incremental.sh'", source)

    def test_validate_blocks_fallback_debezium_and_stuck_lag(self) -> None:
        source = read("deploy/scripts/prod-multivm-validate.sh")
        self.assertIn("REALTIME_MARTS_FALLBACK=true is forbidden", source)
        self.assertIn("connector != 'RUNNING'", source)
        self.assertIn("any(state != 'RUNNING' for state in tasks)", source)
        self.assertIn("CDC_LAG_MAX_MESSAGES", source)
        self.assertIn("cdc.lag.not_stuck", source)
        self.assertIn("data_key=0", source)
        self.assertIn("CRITICAL_DATA_KEY", source)

    def test_proof_json_has_pass_fail_result_contract(self) -> None:
        source = read("deploy/scripts/prod-multivm-proof.sh")
        self.assertIn('"result": os.environ["PROOF_RESULT"]', source)
        self.assertIn('RESULT="PASS"', source)
        self.assertIn('RESULT="FAIL"', source)
        self.assertIn("product_screen_smoke", source)


if __name__ == "__main__":
    unittest.main()
