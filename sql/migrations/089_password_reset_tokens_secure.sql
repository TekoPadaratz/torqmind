-- 089_password_reset_tokens_secure.sql
-- Recuperação de senha segura ("esqueci minha senha").
--
-- A tabela auth.password_reset_tokens já existe (migration 001) mas guardava o
-- token em claro (coluna `token uuid`). Padrão atual de segurança: NUNCA guardar
-- o token em claro. O servidor guarda apenas o SHA-256 do token; o link enviado
-- por e-mail carrega somente o token aleatório (sem e-mail embutido) e a busca é
-- feita pelo hash. Assim, vazamento do banco não expõe tokens utilizáveis.
--
-- Idempotente e seguro (ADD COLUMN IF NOT EXISTS, sem DROP/TRUNCATE).

BEGIN;

-- Garante a tabela base (prod pode ter sido bootstrapado sem ela, mesmo com
-- 001_auth.sql marcada como baseline). Estrutura compatível com 001_auth.sql.
CREATE TABLE IF NOT EXISTS auth.password_reset_tokens (
  token             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id           uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  expires_at        timestamptz NOT NULL,
  used_at           timestamptz NULL,
  created_at        timestamptz NOT NULL DEFAULT now()
);

-- Hash do token (sha256 hex). Único para permitir lookup direto e evitar colisão.
ALTER TABLE auth.password_reset_tokens
  ADD COLUMN IF NOT EXISTS token_hash text;

-- Metadados de auditoria do pedido de recuperação.
ALTER TABLE auth.password_reset_tokens
  ADD COLUMN IF NOT EXISTS requested_ip inet;

ALTER TABLE auth.password_reset_tokens
  ADD COLUMN IF NOT EXISTS requested_user_agent text;

-- A coluna legada `token` continua existindo com default gen_random_uuid()
-- (apenas como identificador interno); o fluxo novo nunca a expõe.
-- Garante unicidade do hash quando preenchido.
CREATE UNIQUE INDEX IF NOT EXISTS uq_password_reset_token_hash
  ON auth.password_reset_tokens (token_hash)
  WHERE token_hash IS NOT NULL;

-- Lookup rápido de tokens ativos por usuário (invalidação dos anteriores).
CREATE INDEX IF NOT EXISTS ix_password_reset_user_active
  ON auth.password_reset_tokens (user_id)
  WHERE used_at IS NULL;

-- Limpeza/expiração.
CREATE INDEX IF NOT EXISTS ix_password_reset_expires_at
  ON auth.password_reset_tokens (expires_at);

COMMIT;
