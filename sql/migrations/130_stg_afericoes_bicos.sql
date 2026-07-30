-- Migration 130: Aferição operacional de bicos (Xpert dbo.AFERICAO)
-- Idempotente. Não destrutivo.
-- Ato de aferir o bico (QTDE + DATA), não validade INMETRO/lacre.

CREATE TABLE IF NOT EXISTS stg.afericoes (
  id_empresa   integer NOT NULL,
  id_filial    integer NOT NULL,
  id_afericao  integer NOT NULL,          -- ID_AFERICAO
  id_db        integer NOT NULL DEFAULT 0,
  payload      jsonb   NOT NULL DEFAULT '{}'::jsonb,
  dt_evento    timestamptz,
  ingested_at  timestamptz NOT NULL DEFAULT now(),
  received_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_afericao)
);

CREATE INDEX IF NOT EXISTS ix_stg_afericoes_scope_evento
  ON stg.afericoes (id_empresa, id_filial, dt_evento);

COMMENT ON TABLE stg.afericoes IS
  'Aferição operacional de bico (Xpert dbo.AFERICAO): payload ID_BICOS, ID_TURNOS, QTDE, DATA, ID_USUARIOS, ID_USUARIOS_LIB.';

-- Cadastro de bicos (rótulos na tela de aferição)
CREATE TABLE IF NOT EXISTS stg.bicos (
  id_empresa  integer NOT NULL,
  id_filial   integer NOT NULL,
  id_bico     integer NOT NULL,           -- ID_BICOS
  id_db       integer NOT NULL DEFAULT 0,
  payload     jsonb   NOT NULL DEFAULT '{}'::jsonb,
  dt_evento   timestamptz,
  ingested_at timestamptz NOT NULL DEFAULT now(),
  received_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_bico)
);

CREATE INDEX IF NOT EXISTS ix_stg_bicos_scope
  ON stg.bicos (id_empresa, id_filial);

COMMENT ON TABLE stg.bicos IS
  'Cadastro de bicos (Xpert dbo.BICOS): payload liga bico→tanque/produto, DATALACRE/NROLACREBOMBA para INMETRO futuro.';
