-- ============================================================================
-- Migration 140: TorqMind Intelligence (chat determinístico somente-leitura)
-- ============================================================================
-- Persistência aditiva + RLS por tenant. Sem DROP/TRUNCATE.
-- O assistente NÃO armazena resultados brutos de tools nem PII sensível.
-- ============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS app.ai_conversations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  id_empresa integer NOT NULL,
  user_id uuid NOT NULL,
  status text NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'archived', 'deleted')),
  title text,
  permission_hash text NOT NULL DEFAULT '',
  branch_scope jsonb NOT NULL DEFAULT '[]'::jsonb,
  context_opaque jsonb NOT NULL DEFAULT '{}'::jsonb,
  message_count integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  archived_at timestamptz,
  last_message_at timestamptz
);

CREATE INDEX IF NOT EXISTS ix_ai_conversations_tenant_user
  ON app.ai_conversations (id_empresa, user_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS app.ai_messages (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_id uuid NOT NULL REFERENCES app.ai_conversations(id) ON DELETE CASCADE,
  id_empresa integer NOT NULL,
  user_id uuid NOT NULL,
  role text NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
  status text NOT NULL DEFAULT 'ok'
    CHECK (status IN (
      'ok', 'clarification_required', 'forbidden', 'unsupported',
      'mutation_denied', 'stale_data', 'no_data', 'timeout',
      'overloaded', 'validation_failed', 'unknown'
    )),
  content_text text NOT NULL,
  content_hash text NOT NULL DEFAULT '',
  intent_id text,
  intent_version text,
  confidence numeric(6,4),
  slots_opaque jsonb NOT NULL DEFAULT '{}'::jsonb,
  evidence_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  deep_link_key text,
  answer_id uuid,
  request_id uuid,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_ai_messages_conversation
  ON app.ai_messages (conversation_id, created_at ASC);
CREATE INDEX IF NOT EXISTS ix_ai_messages_tenant_user
  ON app.ai_messages (id_empresa, user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS app.ai_tool_calls (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_id uuid NOT NULL REFERENCES app.ai_conversations(id) ON DELETE CASCADE,
  message_id uuid REFERENCES app.ai_messages(id) ON DELETE SET NULL,
  id_empresa integer NOT NULL,
  user_id uuid NOT NULL,
  tool_name text NOT NULL,
  tool_version text NOT NULL DEFAULT '1',
  args_minimized jsonb NOT NULL DEFAULT '{}'::jsonb,
  result_hash text,
  result_row_count integer,
  latency_ms integer,
  status text NOT NULL DEFAULT 'ok'
    CHECK (status IN ('ok', 'forbidden', 'timeout', 'error', 'no_data', 'stale_data')),
  source_label text,
  freshness_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_ai_tool_calls_conversation
  ON app.ai_tool_calls (conversation_id, created_at ASC);
CREATE INDEX IF NOT EXISTS ix_ai_tool_calls_tenant
  ON app.ai_tool_calls (id_empresa, created_at DESC);

CREATE TABLE IF NOT EXISTS app.ai_feedback (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_id uuid NOT NULL REFERENCES app.ai_conversations(id) ON DELETE CASCADE,
  message_id uuid NOT NULL REFERENCES app.ai_messages(id) ON DELETE CASCADE,
  id_empresa integer NOT NULL,
  user_id uuid NOT NULL,
  rating smallint NOT NULL CHECK (rating IN (-1, 1)),
  reason_code text,
  note_hash text,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_ai_feedback_message_user UNIQUE (message_id, user_id)
);

CREATE INDEX IF NOT EXISTS ix_ai_feedback_tenant
  ON app.ai_feedback (id_empresa, created_at DESC);

CREATE TABLE IF NOT EXISTS app.ai_tenant_lexicon (
  id bigserial PRIMARY KEY,
  id_empresa integer NOT NULL,
  kind text NOT NULL
    CHECK (kind IN ('alias_filial', 'alias_cliente', 'alias_produto', 'alias_indicador', 'synonym', 'abbreviation')),
  surface_form text NOT NULL,
  normalized_form text NOT NULL,
  target_opaque jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'approved', 'rejected', 'retired')),
  frequency integer NOT NULL DEFAULT 1,
  created_by integer,
  approved_by integer,
  evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  approved_at timestamptz,
  retired_at timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_ai_tenant_lexicon_active
  ON app.ai_tenant_lexicon (id_empresa, kind, normalized_form)
  WHERE status IN ('pending', 'approved');

CREATE INDEX IF NOT EXISTS ix_ai_tenant_lexicon_tenant_status
  ON app.ai_tenant_lexicon (id_empresa, status, kind);

CREATE TABLE IF NOT EXISTS app.ai_verified_questions (
  id bigserial PRIMARY KEY,
  id_empresa integer NOT NULL,
  question_text text NOT NULL,
  question_normalized text NOT NULL,
  intent_id text NOT NULL,
  intent_version text NOT NULL DEFAULT '1',
  slots_template jsonb NOT NULL DEFAULT '{}'::jsonb,
  response_template_key text,
  status text NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'approved', 'rejected', 'retired')),
  created_by integer,
  approved_by integer,
  evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  approved_at timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_ai_verified_questions_active
  ON app.ai_verified_questions (id_empresa, question_normalized)
  WHERE status IN ('pending', 'approved');

CREATE TABLE IF NOT EXISTS app.ai_capability_coverage (
  id bigserial PRIMARY KEY,
  domain text NOT NULL,
  subdomain text,
  intent_id text NOT NULL,
  tool_name text,
  screen_key text,
  coverage_status text NOT NULL DEFAULT 'covered'
    CHECK (coverage_status IN ('covered', 'partial', 'gap', 'unsupported')),
  notes text,
  catalog_version text NOT NULL DEFAULT '1',
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_ai_capability_coverage UNIQUE (intent_id, catalog_version)
);

CREATE TABLE IF NOT EXISTS app.ai_unknown_questions_queue (
  id bigserial PRIMARY KEY,
  id_empresa integer NOT NULL,
  user_id uuid,
  question_text text NOT NULL,
  question_normalized text NOT NULL,
  question_hash text NOT NULL,
  permission_hash text,
  status text NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'reviewing', 'converted', 'rejected', 'duplicate')),
  frequency integer NOT NULL DEFAULT 1,
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_ai_unknown_questions_hash
  ON app.ai_unknown_questions_queue (id_empresa, question_hash);

CREATE INDEX IF NOT EXISTS ix_ai_unknown_questions_status
  ON app.ai_unknown_questions_queue (id_empresa, status, last_seen_at DESC);

-- RLS (defesa em profundidade; API continua filtrando por tenant/user)
ALTER TABLE app.ai_conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.ai_conversations FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rls_tenant_ai_conversations ON app.ai_conversations;
CREATE POLICY rls_tenant_ai_conversations ON app.ai_conversations
  USING (app.rls_tenant_check(id_empresa));

ALTER TABLE app.ai_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.ai_messages FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rls_tenant_ai_messages ON app.ai_messages;
CREATE POLICY rls_tenant_ai_messages ON app.ai_messages
  USING (app.rls_tenant_check(id_empresa));

ALTER TABLE app.ai_tool_calls ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.ai_tool_calls FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rls_tenant_ai_tool_calls ON app.ai_tool_calls;
CREATE POLICY rls_tenant_ai_tool_calls ON app.ai_tool_calls
  USING (app.rls_tenant_check(id_empresa));

ALTER TABLE app.ai_feedback ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.ai_feedback FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rls_tenant_ai_feedback ON app.ai_feedback;
CREATE POLICY rls_tenant_ai_feedback ON app.ai_feedback
  USING (app.rls_tenant_check(id_empresa));

ALTER TABLE app.ai_tenant_lexicon ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.ai_tenant_lexicon FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rls_tenant_ai_tenant_lexicon ON app.ai_tenant_lexicon;
CREATE POLICY rls_tenant_ai_tenant_lexicon ON app.ai_tenant_lexicon
  USING (app.rls_tenant_check(id_empresa));

ALTER TABLE app.ai_verified_questions ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.ai_verified_questions FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rls_tenant_ai_verified_questions ON app.ai_verified_questions;
CREATE POLICY rls_tenant_ai_verified_questions ON app.ai_verified_questions
  USING (app.rls_tenant_check(id_empresa));

ALTER TABLE app.ai_unknown_questions_queue ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.ai_unknown_questions_queue FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rls_tenant_ai_unknown_questions_queue ON app.ai_unknown_questions_queue;
CREATE POLICY rls_tenant_ai_unknown_questions_queue ON app.ai_unknown_questions_queue
  USING (app.rls_tenant_check(id_empresa));

-- coverage é catálogo global (sem id_empresa); sem RLS de tenant

COMMIT;
