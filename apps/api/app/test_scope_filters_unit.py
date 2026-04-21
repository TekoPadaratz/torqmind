from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi import HTTPException

from app import scope


class _RowsCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _ConnStub:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, sql, params):  # noqa: ARG002
        return _RowsCursor(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: ARG002
        return False


class ScopeFiltersUnitTest(unittest.TestCase):
    def _company_claims(self) -> dict:
        return {
            "role": "OWNER",
            "user_role": "tenant_admin",
            "id_empresa": 42,
            "id_filial": None,
            "tenant_ids": [42],
            "access": {"product": True},
            "accesses": [
                {
                    "id_empresa": 42,
                    "id_filial": None,
                    "role": "tenant_admin",
                }
            ],
        }

    def test_all_branches_scope_materializes_only_active_authorized_branches(self) -> None:
        claims = self._company_claims()

        with patch("app.scope.get_conn", return_value=_ConnStub([{"id_filial": 101}, {"id_filial": 103}])):
            tenant_id, branch_filter, branch_scope = scope.resolve_scope_filters(claims, id_empresa_q=42)

        self.assertEqual(tenant_id, 42)
        self.assertEqual(branch_filter, [101, 103])
        self.assertIsNone(branch_scope)

    def test_explicit_multi_branch_scope_rejects_inactive_branch(self) -> None:
        claims = self._company_claims()

        with patch("app.scope.get_conn", return_value=_ConnStub([{"id_filial": 101}, {"id_filial": 103}])):
            with self.assertRaises(HTTPException) as exc:
                scope.resolve_scope_filters(
                    claims,
                    id_empresa_q=42,
                    id_filiais_q=[101, 102],
                )

        self.assertEqual(exc.exception.status_code, 403)
        self.assertEqual(exc.exception.detail["error"], "branch_access_denied")

    def test_branch_specific_access_is_intersected_with_active_directory(self) -> None:
        claims = {
            "role": "MANAGER",
            "user_role": "tenant_manager",
            "id_empresa": 42,
            "id_filial": 101,
            "tenant_ids": [42],
            "access": {"product": True},
            "accesses": [
                {"id_empresa": 42, "id_filial": 101, "role": "tenant_manager"},
                {"id_empresa": 42, "id_filial": 102, "role": "tenant_manager"},
            ],
        }

        with patch("app.scope.get_conn", return_value=_ConnStub([{"id_filial": 101}])):
            can_list_all, branch_ids = scope.accessible_branch_ids(claims, 42)
            tenant_id, branch_filter, branch_scope = scope.resolve_scope_filters(claims, id_empresa_q=42)

        self.assertFalse(can_list_all)
        self.assertEqual(branch_ids, [101])
        self.assertEqual(tenant_id, 42)
        self.assertEqual(branch_filter, 101)
        self.assertEqual(branch_scope, [101])

    def test_all_branches_with_no_active_branch_returns_empty_effective_scope(self) -> None:
        claims = self._company_claims()

        with patch("app.scope.get_conn", return_value=_ConnStub([])):
            tenant_id, branch_filter, branch_scope = scope.resolve_scope_filters(claims, id_empresa_q=42)

        self.assertEqual(tenant_id, 42)
        self.assertEqual(branch_filter, [])
        self.assertIsNone(branch_scope)


if __name__ == "__main__":
    unittest.main()
