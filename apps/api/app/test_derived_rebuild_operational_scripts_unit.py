from __future__ import annotations

import unittest
from pathlib import Path


def repo_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "deploy" / "scripts").is_dir():
            return parent
    raise unittest.SkipTest("repository deploy scripts are not available in this runtime")


def read(path: str) -> str:
    return (repo_root() / path).read_text(encoding="utf-8")


def mode(path: str) -> int:
    return (repo_root() / path).stat().st_mode & 0o777


class DerivedRebuildOperationalScriptsTest(unittest.TestCase):
    def test_incremental_wrapper_exposes_controlled_rebuild_flags(self) -> None:
        source = read("deploy/scripts/prod-etl-incremental.sh")
        self.assertIn("--from-date", source)
        self.assertIn("--to-date", source)
        self.assertIn("--branch-id", source)
        self.assertIn("--force-full", source)
        self.assertIn("FORCE_FULL", source)

    def test_rebuild_script_preserves_staging_and_targets_only_derived_tables(self) -> None:
        source = read("deploy/scripts/prod-rebuild-derived-from-stg.sh")
        self.assertIn('FROM_DATE="${FROM_DATE:-2025-01-01}"', source)
        self.assertIn("stg.comprovantes", source)
        self.assertIn("stg.itenscomprovantes", source)
        self.assertIn('delete_scope "dw.fact_venda"', source)
        self.assertIn('delete_scope "dw.fact_venda_item"', source)
        self.assertIn('delete_scope "dw.fact_pagamento_comprovante"', source)
        self.assertIn("prod-etl-incremental.sh", source)
        self.assertNotIn("DELETE FROM stg.comprovantes", source)
        self.assertNotIn("DELETE FROM stg.itenscomprovantes", source)

    def test_rebuild_script_is_executable(self) -> None:
        self.assertEqual(mode("deploy/scripts/prod-rebuild-derived-from-stg.sh"), 0o755)

    def test_rebuild_script_has_safety_and_verification_flags(self) -> None:
        source = read("deploy/scripts/prod-rebuild-derived-from-stg.sh")
        self.assertIn("--include-dimensions", source)
        self.assertIn("--skip-purge", source)
        self.assertIn("--skip-etl", source)
        self.assertIn("--skip-verify", source)
        self.assertIn("--dry-run", source)
        self.assertIn("STG coverage does not reach FROM_DATE", source)
        self.assertIn("--include-dimensions requires tenant-wide open-ended rebuild", source)
        self.assertIn('delete_dimension_scope "dw.dim_produto"', source)
        self.assertIn("tm_require_prod_runtime_env", source)

    def test_runtime_scope_migration_adds_window_and_branch_helpers(self) -> None:
        source = read("sql/migrations/072_derived_rebuild_runtime_scope.sql")
        self.assertIn("current_setting('etl.from_date', true)", source)
        self.assertIn("current_setting('etl.to_date', true)", source)
        self.assertIn("current_setting('etl.branch_id', true)", source)
        self.assertIn("current_setting('etl.force_full_scan', true)", source)
        self.assertIn("runtime_from_date", source)
        self.assertIn("runtime_to_date", source)
        self.assertIn("runtime_branch_id", source)
        self.assertIn("runtime_force_full_scan", source)
        self.assertIn("runtime_watermark_updates_enabled", source)
        self.assertIn("runtime_branch_matches", source)
        self.assertIn("runtime_business_date_in_range", source)
        self.assertIn("etl.sales_cutoff_date", source)
        self.assertNotIn("stg.movprodutos", source)

    def test_runtime_scope_migration_overrides_public_sales_wrappers(self) -> None:
        source = read("sql/migrations/072_derived_rebuild_runtime_scope.sql")
        self.assertIn("CREATE OR REPLACE FUNCTION etl.load_fact_pagamento_comprovante_detail", source)
        self.assertIn("CREATE OR REPLACE FUNCTION etl.load_fact_pagamento_comprovante(p_id_empresa int)", source)
        self.assertIn("CREATE OR REPLACE FUNCTION etl.load_fact_venda_item_detail", source)
        self.assertIn("CREATE OR REPLACE FUNCTION etl.load_fact_venda_item(p_id_empresa int)", source)
        self.assertIn("etl.load_fact_pagamento_comprovante_range_detail", source)
        self.assertIn("etl.load_fact_venda_item_range_detail", source)
        self.assertIn("etl.runtime_watermark_updates_enabled()", source)

    def test_runtime_scope_migration_uses_window_filters_in_hot_path_queries(self) -> None:
        source = read("sql/migrations/072_derived_rebuild_runtime_scope.sql")
        self.assertIn("CREATE OR REPLACE FUNCTION etl.load_fact_comprovante", source)
        self.assertIn("CREATE OR REPLACE FUNCTION etl.load_fact_venda", source)
        self.assertIn("CREATE OR REPLACE FUNCTION etl.fact_venda_item_pending_bounds", source)
        self.assertIn("CREATE OR REPLACE FUNCTION etl.load_fact_venda_item_range_detail", source)
        self.assertIn("CREATE OR REPLACE FUNCTION etl.fact_pagamento_comprovante_pending_bounds", source)
        self.assertIn("CREATE OR REPLACE FUNCTION etl.load_fact_pagamento_comprovante_range_detail", source)
        self.assertIn("CREATE OR REPLACE FUNCTION etl.load_fact_caixa_turno", source)
        self.assertIn("CREATE OR REPLACE FUNCTION etl.load_fact_financeiro", source)
        self.assertIn("etl.runtime_business_date_in_range(", source)
        self.assertIn("etl.runtime_branch_matches(", source)
        self.assertIn("etl.runtime_force_full_scan()", source)

    def test_homologation_apply_integrates_derived_rebuild_guard_rails(self) -> None:
        source = read("deploy/scripts/prod-homologation-apply.sh")
        self.assertIn("--rebuild-dw-from-stg", source)
        self.assertIn("--skip-derived-rebuild", source)
        self.assertIn("--include-dimensions", source)
        self.assertIn("--from-date", source)
        self.assertIn("--to-date", source)
        self.assertIn("--allow-dw-only", source)
        self.assertIn("step_derived_rebuild", source)
        self.assertIn("FULL_CLICKHOUSE=1", source)
        self.assertIn("cannot be combined with --skip-migrate", source)
        self.assertIn("unless --allow-dw-only is used", source)

    def test_homologation_apply_separates_audit_filial_from_rebuild_filial(self) -> None:
        source = read("deploy/scripts/prod-homologation-apply.sh")
        self.assertIn('REBUILD_ID_FILIAL=""', source)
        self.assertIn("--rebuild-id-filial", source)
        self.assertIn("--all-filiais", source)
        self.assertIn("audit_filial=", source)
        self.assertIn("rebuild_filial=", source)
        self.assertIn("--all-filiais and --rebuild-id-filial cannot be used together", source)
        self.assertIn("--rebuild-id-filial must be numeric", source)
        self.assertIn("--include-dimensions requires tenant-wide open-ended rebuild", source)
        # step_derived_rebuild passes REBUILD_ID_FILIAL, not ID_FILIAL
        self.assertIn('ID_FILIAL="$REBUILD_ID_FILIAL"', source)

    def test_history_coverage_audit_reports_monthly_stg_dw_clickhouse_and_gaps(self) -> None:
        source = read("deploy/scripts/prod-history-coverage-audit.sh")
        self.assertIn("Monthly counts PostgreSQL STG", source)
        self.assertIn("Monthly counts PostgreSQL DW", source)
        self.assertIn("Monthly counts ClickHouse DW", source)
        self.assertIn("Monthly counts ClickHouse mart", source)
        self.assertIn("emit_gap_line", source)
        self.assertIn('print_monthly_section "stg.comprovantes"', source)
        self.assertIn("torqmind_dw.fact_venda", source)
        self.assertIn("torqmind_mart.agg_vendas_diaria", source)


if __name__ == "__main__":
    unittest.main()