-- Phase 5: Jarvis AI cache + usage telemetry (additive, contract-safe)

CREATE TABLE IF NOT EXISTS app.insight_ai_cache (
  id bigserial PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),
  id_empresa integer NOT NULL,
  id_filial integer,
  insight_hash text NOT NULL,
  model text NOT NULL,
  response_json jsonb NOT NULL,
  prompt_tokens integer NOT NULL DEFAULT 0,
  completion_tokens integer NOT NULL DEFAULT 0,
  estimated_cost_usd numeric(18,8) NOT NULL DEFAULT 0,
  source text NOT NULL DEFAULT 'openai' CHECK (source IN ('openai','deterministic')),
  error text,
  CONSTRAINT uq_insight_ai_cache UNIQUE (id_empresa, id_filial, insight_hash, model)
);

CREATE INDEX IF NOT EXISTS ix_insight_ai_cache_lookup
  ON app.insight_ai_cache (id_empresa, id_filial, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_insight_ai_cache_model
  ON app.insight_ai_cache (id_empresa, model, created_at DESC);

ALTER TABLE app.insights_gerados
  ADD COLUMN IF NOT EXISTS ai_plan jsonb,
  ADD COLUMN IF NOT EXISTS ai_model text,
  ADD COLUMN IF NOT EXISTS ai_prompt_tokens integer,
  ADD COLUMN IF NOT EXISTS ai_completion_tokens integer,
  ADD COLUMN IF NOT EXISTS ai_generated_at timestamptz,
  ADD COLUMN IF NOT EXISTS ai_cache_hit boolean,
  ADD COLUMN IF NOT EXISTS ai_error text;

CREATE INDEX IF NOT EXISTS ix_insights_gerados_ai_generated
  ON app.insights_gerados (id_empresa, id_filial, ai_generated_at DESC);
