-- 125_preco_fixo_alertas_config.sql
-- Alertas Telegram configuráveis + dedupe preço fixo + exclusão caixa distribuidora.

BEGIN;

-- ---------------------------------------------------------------------------
-- 1) Filiais: excluir alerta de caixa aberto >24h (distribuidoras / sem tanque)
-- ---------------------------------------------------------------------------
ALTER TABLE auth.filiais
  ADD COLUMN IF NOT EXISTS excluir_alerta_caixa boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN auth.filiais.excluir_alerta_caixa IS
  'Quando true, a filial não entra em mart.alerta_caixa_aberto / Telegram CASH_OPEN_OVER_24H.';

UPDATE auth.filiais
SET excluir_alerta_caixa = true
WHERE id_filial IN (14126, 14779, 14780, 14930, 15121, 17719);

-- ---------------------------------------------------------------------------
-- 2) Preferências de alerta por empresa (Telegram company channel)
-- ---------------------------------------------------------------------------
ALTER TABLE app.telegram_settings
  ADD COLUMN IF NOT EXISTS preco_fixo_alerta_base text NOT NULL DEFAULT 'venda',
  ADD COLUMN IF NOT EXISTS alert_venda_cancelada boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS alert_nfe_inutilizada boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS alert_cash_open_over_24h boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS alert_preco_fixo_bomba boolean NOT NULL DEFAULT true;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ck_telegram_settings_preco_fixo_alerta_base'
  ) THEN
    ALTER TABLE app.telegram_settings
      ADD CONSTRAINT ck_telegram_settings_preco_fixo_alerta_base
      CHECK (preco_fixo_alerta_base IN ('venda', 'custo'));
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- 3) Dedupe ouro: preço bomba × preço fixo
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app.alert_preco_fixo_bomba (
  id_empresa              integer NOT NULL,
  id_filial               integer NOT NULL,
  id_entidade             integer NOT NULL,
  id_produto              integer NOT NULL,
  base_ref                text NOT NULL DEFAULT 'venda',
  preco_fixo             numeric(18, 4) NOT NULL DEFAULT 0,
  preco_ref_na_notificacao numeric(18, 4) NOT NULL DEFAULT 0,
  notificado_em           timestamptz NOT NULL DEFAULT now(),
  payload                 jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (id_empresa, id_filial, id_entidade, id_produto)
);

CREATE INDEX IF NOT EXISTS ix_alert_preco_fixo_bomba_empresa
  ON app.alert_preco_fixo_bomba (id_empresa, notificado_em DESC);

COMMENT ON TABLE app.alert_preco_fixo_bomba IS
  'Dedupe Telegram PRECO_FIXO_BOMBA_DESATUALIZADO: re-dispara só se a referência subir sem reajuste do fixo.';

-- ---------------------------------------------------------------------------
-- 4) Seed assinaturas tipadas (ON) para quem já tem Telegram ativo
-- ---------------------------------------------------------------------------
INSERT INTO app.notification_subscriptions (
  user_id, tenant_id, branch_id, event_type, channel, severity_min, is_enabled
)
SELECT
  s.user_id,
  ut.id_empresa,
  NULL,
  e.event_type,
  'telegram',
  'CRITICAL',
  true
FROM app.user_notification_settings s
INNER JOIN auth.user_tenants ut
  ON ut.user_id = s.user_id
CROSS JOIN (
  VALUES
    ('VENDA_CANCELADA'),
    ('NFE_INUTILIZADA'),
    ('CASH_OPEN_OVER_24H'),
    ('PRECO_FIXO_BOMBA_DESATUALIZADO')
) AS e(event_type)
WHERE COALESCE(s.telegram_enabled, false) = true
  AND s.telegram_chat_id IS NOT NULL
  AND btrim(s.telegram_chat_id) <> ''
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- 5) Mart alerta caixa: mensagem operacional + exclusão distribuidora
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mart.alerta_caixa_aberto CASCADE;

CREATE MATERIALIZED VIEW mart.alerta_caixa_aberto AS
SELECT
  a.id_empresa,
  a.id_filial,
  a.filial_nome,
  a.id_turno,
  a.id_usuario,
  a.usuario_nome,
  a.abertura_ts,
  a.last_activity_ts,
  a.horas_aberto,
  'CRITICAL'::text AS severity,
  format(
    'Caixa aberto há %s h — %s',
    trim(to_char(a.horas_aberto, 'FM999999990D00')),
    COALESCE(NULLIF(f.apelido, ''), NULLIF(a.filial_nome, ''), format('Filial %s', a.id_filial))
  ) AS title,
  format(
    'Atenção: o caixa da filial %s está aberto há %s horas (desde %s). Operador: %s. Última atividade: %s. Confira o fechamento no TorqMind (/cash).',
    COALESCE(NULLIF(f.apelido, ''), NULLIF(a.filial_nome, ''), format('Filial %s', a.id_filial)),
    trim(to_char(a.horas_aberto, 'FM999999990D00')),
    COALESCE(to_char(a.abertura_ts AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY HH24:MI'), '—'),
    COALESCE(NULLIF(a.usuario_nome, ''), 'não identificado'),
    COALESCE(to_char(a.last_activity_ts AT TIME ZONE 'America/Sao_Paulo', 'DD/MM/YYYY HH24:MI'), 'sem atividade')
  ) AS body,
  '/cash'::text AS url,
  (
    ('x' || substr(md5(
      'CASH_OPEN_OVER_24H|' || a.id_empresa::text || '|' || a.id_filial::text || '|' || a.id_turno::text
    ), 1, 16))::bit(64)::bigint
  ) AS insight_id_hash,
  now() AS updated_at
FROM mart.agg_caixa_turno_aberto a
LEFT JOIN auth.filiais f
  ON f.id_empresa = a.id_empresa
 AND f.id_filial = a.id_filial
WHERE a.is_operational_live = true
  AND a.horas_aberto >= 24
  AND COALESCE(f.excluir_alerta_caixa, false) = false;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mart_alerta_caixa_aberto
  ON mart.alerta_caixa_aberto (id_empresa, id_filial, id_turno);
CREATE INDEX IF NOT EXISTS ix_mart_alerta_caixa_aberto_lookup
  ON mart.alerta_caixa_aberto (id_empresa, severity, horas_aberto DESC);

REFRESH MATERIALIZED VIEW mart.alerta_caixa_aberto;

COMMIT;
