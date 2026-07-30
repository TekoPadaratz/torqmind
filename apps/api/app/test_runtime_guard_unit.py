"""Unit tests for prod/homolog runtime stack guard."""

from __future__ import annotations

import pytest

from app.runtime_guard import assert_runtime_stack_or_exit, _database_name_from_url


def test_database_name_from_url():
    assert _database_name_from_url("postgresql://u:p@h:5432/torqmind") == "torqmind"
    assert _database_name_from_url("postgresql://u:p@h:5432/torqmind_homolog?sslmode=disable") == "torqmind_homolog"
    assert _database_name_from_url("") == ""


def test_prod_stack_rejects_homolog_database(monkeypatch):
    monkeypatch.setenv("TORQMIND_STACK", "prod")
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/torqmind_homolog")
    with pytest.raises(SystemExit) as exc:
        assert_runtime_stack_or_exit()
    assert exc.value.code == 2


def test_prod_stack_rejects_homolog_app_env(monkeypatch):
    monkeypatch.setenv("TORQMIND_STACK", "prod")
    monkeypatch.setenv("APP_ENV", "homolog")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/torqmind")
    with pytest.raises(SystemExit):
        assert_runtime_stack_or_exit()


def test_prod_stack_accepts_prod_database(monkeypatch):
    monkeypatch.setenv("TORQMIND_STACK", "prod")
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/torqmind")
    monkeypatch.setenv("PG_DATABASE", "torqmind")
    assert_runtime_stack_or_exit()


def test_homolog_stack_rejects_prod_database(monkeypatch):
    monkeypatch.setenv("TORQMIND_STACK", "homolog")
    monkeypatch.setenv("APP_ENV", "homolog")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/torqmind")
    monkeypatch.setenv("PG_DATABASE", "torqmind")
    with pytest.raises(SystemExit):
        assert_runtime_stack_or_exit()


def test_no_stack_is_noop_for_tests(monkeypatch):
    monkeypatch.delenv("TORQMIND_STACK", raising=False)
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/torqmind_homolog")
    assert_runtime_stack_or_exit()
