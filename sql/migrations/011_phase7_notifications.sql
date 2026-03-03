-- Phase 7: In-app notifications

CREATE TABLE IF NOT EXISTS app.notifications (
  id bigserial PRIMARY KEY,
  id_empresa integer NOT NULL,
  id_filial integer,
  insight_id bigint,
  severity text NOT NULL CHECK (severity IN ('INFO','WARN','CRITICAL')),
  title text NOT NULL,
  body text NOT NULL,
  url text,
  created_at timestamptz NOT NULL DEFAULT now(),
  read_at timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_notifications_insight
  ON app.notifications (id_empresa, id_filial, insight_id)
  WHERE insight_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_notifications_lookup
  ON app.notifications (id_empresa, id_filial, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_notifications_unread
  ON app.notifications (id_empresa, id_filial, read_at, created_at DESC);
