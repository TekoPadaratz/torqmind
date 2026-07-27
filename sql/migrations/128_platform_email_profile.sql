-- Migration 128: perfil de e-mail administrativo da plataforma (não-segredo).
-- Credenciais SMTP permanecem em env do servidor (/etc/torqmind/*.app.env).
-- Singleton: id = 1.

CREATE TABLE IF NOT EXISTS app.platform_email_profile (
  id              smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  channel_name    text NOT NULL DEFAULT 'TorqMind',
  contact_name    text,
  from_email      text,
  updated_at      timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE app.platform_email_profile IS
  'Perfil de apresentação do e-mail administrativo TorqMind (nome/contato/from). SMTP secret só no env.';

INSERT INTO app.platform_email_profile (id, channel_name, contact_name, from_email)
VALUES (1, 'TorqMind', NULL, 'torqmind@hlsolucao.com.br')
ON CONFLICT (id) DO NOTHING;
