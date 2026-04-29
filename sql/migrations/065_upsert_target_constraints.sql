-- ============================================================================
-- Migration 065: UPSERT target constraints for drifted databases
-- ============================================================================
-- PT-BR:
-- Bancos antigos podem ter tabelas stg/app/billing sem índices únicos que a API
-- usa como alvos de ON CONFLICT. Esta migration cria os índices de forma
-- idempotente e falha explicitamente se houver duplicidade real a reconciliar.
-- ============================================================================

BEGIN;

DO $$
DECLARE
  spec record;
  table_reg regclass;
  cols_sql text;
  has_dups boolean;
BEGIN
  FOR spec IN
    SELECT *
    FROM (
      VALUES
        ('stg.filiais', 'uq_stg_filiais_upsert', ARRAY['id_empresa', 'id_filial']),
        ('stg.funcionarios', 'uq_stg_funcionarios_upsert', ARRAY['id_empresa', 'id_filial', 'id_funcionario']),
        ('stg.usuarios', 'uq_stg_usuarios_upsert', ARRAY['id_empresa', 'id_filial', 'id_usuario']),
        ('stg.entidades', 'uq_stg_entidades_upsert', ARRAY['id_empresa', 'id_filial', 'id_entidade']),
        ('stg.grupoprodutos', 'uq_stg_grupoprodutos_upsert', ARRAY['id_empresa', 'id_filial', 'id_grupoprodutos']),
        ('stg.localvendas', 'uq_stg_localvendas_upsert', ARRAY['id_empresa', 'id_filial', 'id_localvendas']),
        ('stg.produtos', 'uq_stg_produtos_upsert', ARRAY['id_empresa', 'id_filial', 'id_produto']),
        ('stg.turnos', 'uq_stg_turnos_upsert', ARRAY['id_empresa', 'id_filial', 'id_turno']),
        ('stg.comprovantes', 'uq_stg_comprovantes_upsert', ARRAY['id_empresa', 'id_filial', 'id_db', 'id_comprovante']),
        ('stg.movprodutos', 'uq_stg_movprodutos_upsert', ARRAY['id_empresa', 'id_filial', 'id_db', 'id_movprodutos']),
        ('stg.movlctos', 'uq_stg_movlctos_upsert', ARRAY['id_empresa', 'id_filial', 'id_db', 'id_movlctos']),
        ('stg.itensmovprodutos', 'uq_stg_itensmovprodutos_upsert', ARRAY['id_empresa', 'id_filial', 'id_db', 'id_movprodutos', 'id_itensmovprodutos']),
        ('stg.itenscomprovantes', 'uq_stg_itenscomprovantes_upsert', ARRAY['id_empresa', 'id_filial', 'id_db', 'id_comprovante', 'id_itemcomprovante']),
        ('stg.formas_pgto_comprovantes', 'uq_stg_formas_pgto_comprovantes_upsert', ARRAY['id_empresa', 'id_filial', 'id_referencia', 'tipo_forma']),
        ('stg.estoque', 'uq_stg_estoque_upsert', ARRAY['id_empresa', 'id_filial', 'id_estoque']),
        ('stg.contaspagar', 'uq_stg_contaspagar_upsert', ARRAY['id_empresa', 'id_filial', 'id_db', 'id_contaspagar']),
        ('stg.contasreceber', 'uq_stg_contasreceber_upsert', ARRAY['id_empresa', 'id_filial', 'id_db', 'id_contasreceber']),
        ('stg.financeiro', 'uq_stg_financeiro_upsert', ARRAY['id_empresa', 'id_filial', 'id_db', 'tipo_titulo', 'id_titulo']),
        ('app.goals', 'uq_app_goals_scope', ARRAY['id_empresa', 'id_filial', 'goal_date', 'goal_type']),
        ('billing.receivables', 'uq_billing_receivables_tenant_contract_competence', ARRAY['tenant_id', 'contract_id', 'competence_month']),
        ('billing.channel_payables', 'uq_billing_channel_payables_receivable', ARRAY['receivable_id'])
    ) AS specs(table_name, index_name, cols)
  LOOP
    table_reg := to_regclass(spec.table_name);
    IF table_reg IS NULL THEN
      CONTINUE;
    END IF;

    SELECT string_agg(format('%I', col), ', ')
      INTO cols_sql
    FROM unnest(spec.cols) AS col;

    EXECUTE format(
      'SELECT EXISTS (SELECT 1 FROM %s GROUP BY %s HAVING COUNT(*) > 1)',
      table_reg,
      cols_sql
    )
      INTO has_dups;

    IF has_dups THEN
      RAISE EXCEPTION 'Cannot create unique index %.% on %: duplicate rows exist',
        split_part(spec.table_name, '.', 1),
        spec.index_name,
        spec.table_name;
    END IF;

    EXECUTE format(
      'CREATE UNIQUE INDEX IF NOT EXISTS %I ON %s (%s)',
      spec.index_name,
      table_reg,
      cols_sql
    );
  END LOOP;
END $$;

DO $$
BEGIN
  IF to_regclass('app.notifications') IS NOT NULL
     AND to_regclass('app.uq_app_notifications_insight_scope') IS NULL THEN
    IF EXISTS (
      SELECT 1
      FROM app.notifications
      WHERE insight_id IS NOT NULL
      GROUP BY id_empresa, id_filial, insight_id
      HAVING COUNT(*) > 1
    ) THEN
      RAISE EXCEPTION 'Cannot create unique index app.notifications(id_empresa, id_filial, insight_id): duplicate rows exist';
    END IF;

    CREATE UNIQUE INDEX uq_app_notifications_insight_scope
      ON app.notifications (id_empresa, id_filial, insight_id)
      WHERE insight_id IS NOT NULL;
  END IF;
END $$;

COMMIT;
