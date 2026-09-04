#!/usr/bin/env python3
"""Replica tenant Rede Quatizada (id_empresa=8) de PROD → HOMOLOG (app.tenants + auth.filiais)."""
from __future__ import annotations

import os
import sys

import psycopg
from psycopg.types.json import Jsonb


def _norm(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://").replace(
        "postgresql+psycopg2://", "postgresql://"
    )


def _jsonish(value):
    if isinstance(value, (dict, list)):
        return Jsonb(value)
    return value


def main() -> int:
    id_empresa = int(os.environ.get("ID_EMPRESA", "8"))
    prod_url = _norm(os.environ["PROD_DATABASE_URL"])
    hom_url = _norm(os.environ["HOMOLOG_DATABASE_URL"])

    with psycopg.connect(prod_url) as prod, psycopg.connect(hom_url) as hom:
        with prod.cursor() as cur:
            cur.execute(
                """
                SELECT id_empresa, nome, ingest_key, source_system, is_active, created_at,
                       metadata, cnpj, status, valid_from, valid_until, billing_status,
                       grace_until, suspended_reason, suspended_at, reactivated_at,
                       channel_id, plan_name, monthly_amount, billing_day, issue_day,
                       updated_at, sales_history_days, default_product_scope_days, module_tier
                FROM app.tenants WHERE id_empresa = %s
                """,
                (id_empresa,),
            )
            row = cur.fetchone()
            if not row:
                print("tenant not in prod", id_empresa)
                return 1
            cols = [d.name for d in cur.description]
            tenant = dict(zip(cols, row))
            tenant["metadata"] = _jsonish(tenant.get("metadata"))
            cur.execute(
                """
                SELECT id_empresa, id_filial, nome, is_active, valid_from, valid_until,
                       cnpj, apelido, module_tier, blocked_reason, excluir_alerta_caixa
                FROM auth.filiais WHERE id_empresa = %s ORDER BY id_filial
                """,
                (id_empresa,),
            )
            fcols = [d.name for d in cur.description]
            filiais = [dict(zip(fcols, r)) for r in cur.fetchall()]

        print("prod", tenant["id_empresa"], tenant["nome"], "filiais", len(filiais))

        with hom.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app.tenants (
                  id_empresa, nome, ingest_key, source_system, is_active, created_at,
                  metadata, cnpj, status, valid_from, valid_until, billing_status,
                  grace_until, suspended_reason, suspended_at, reactivated_at,
                  channel_id, plan_name, monthly_amount, billing_day, issue_day,
                  updated_at, sales_history_days, default_product_scope_days, module_tier
                ) VALUES (
                  %(id_empresa)s, %(nome)s, %(ingest_key)s, %(source_system)s, %(is_active)s, %(created_at)s,
                  %(metadata)s, %(cnpj)s, %(status)s, %(valid_from)s, %(valid_until)s, %(billing_status)s,
                  %(grace_until)s, %(suspended_reason)s, %(suspended_at)s, %(reactivated_at)s,
                  %(channel_id)s, %(plan_name)s, %(monthly_amount)s, %(billing_day)s, %(issue_day)s,
                  COALESCE(%(updated_at)s, now()), %(sales_history_days)s, %(default_product_scope_days)s, %(module_tier)s
                )
                ON CONFLICT (id_empresa) DO UPDATE SET
                  nome = EXCLUDED.nome,
                  ingest_key = EXCLUDED.ingest_key,
                  source_system = EXCLUDED.source_system,
                  is_active = EXCLUDED.is_active,
                  metadata = EXCLUDED.metadata,
                  cnpj = EXCLUDED.cnpj,
                  status = EXCLUDED.status,
                  valid_from = EXCLUDED.valid_from,
                  valid_until = EXCLUDED.valid_until,
                  billing_status = EXCLUDED.billing_status,
                  channel_id = EXCLUDED.channel_id,
                  plan_name = EXCLUDED.plan_name,
                  monthly_amount = EXCLUDED.monthly_amount,
                  billing_day = EXCLUDED.billing_day,
                  issue_day = EXCLUDED.issue_day,
                  updated_at = now(),
                  sales_history_days = EXCLUDED.sales_history_days,
                  default_product_scope_days = EXCLUDED.default_product_scope_days,
                  module_tier = EXCLUDED.module_tier
                """,
                tenant,
            )
            for f in filiais:
                cur.execute(
                    """
                    INSERT INTO auth.filiais (
                      id_empresa, id_filial, nome, is_active, valid_from, valid_until,
                      cnpj, apelido, module_tier, blocked_reason, excluir_alerta_caixa
                    ) VALUES (
                      %(id_empresa)s, %(id_filial)s, %(nome)s, %(is_active)s, %(valid_from)s, %(valid_until)s,
                      %(cnpj)s, %(apelido)s, %(module_tier)s, %(blocked_reason)s, %(excluir_alerta_caixa)s
                    )
                    ON CONFLICT (id_empresa, id_filial) DO UPDATE SET
                      nome = EXCLUDED.nome,
                      is_active = EXCLUDED.is_active,
                      valid_from = EXCLUDED.valid_from,
                      valid_until = EXCLUDED.valid_until,
                      cnpj = EXCLUDED.cnpj,
                      apelido = EXCLUDED.apelido,
                      module_tier = EXCLUDED.module_tier,
                      blocked_reason = EXCLUDED.blocked_reason,
                      excluir_alerta_caixa = EXCLUDED.excluir_alerta_caixa,
                      updated_at = now()
                    """,
                    f,
                )
            cur.execute(
                """
                SELECT setval(
                  pg_get_serial_sequence('app.tenants', 'id_empresa'),
                  GREATEST(COALESCE((SELECT MAX(id_empresa) FROM app.tenants), 0), 1),
                  true
                )
                """
            )
            cur.execute(
                "SELECT id_empresa, nome FROM app.tenants WHERE id_empresa = %s",
                (id_empresa,),
            )
            print("homolog_tenant", cur.fetchone())
            cur.execute(
                "SELECT id_filial, nome FROM auth.filiais WHERE id_empresa = %s ORDER BY 1",
                (id_empresa,),
            )
            print("homolog_filiais", cur.fetchall())
        hom.commit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
