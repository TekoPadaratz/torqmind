-- Migration 082: Tabela de parâmetros configuráveis por filial
-- Idempotente: usa IF NOT EXISTS

CREATE TABLE IF NOT EXISTS app.filial_params (
  id_empresa    integer NOT NULL,
  id_filial     integer NOT NULL,
  abc_threshold_a  smallint NOT NULL DEFAULT 80,
  abc_threshold_b  smallint NOT NULL DEFAULT 95,
  abc_exclude_fuel boolean NOT NULL DEFAULT true,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial)
);

COMMENT ON TABLE app.filial_params IS 'Parâmetros configuráveis por filial (ABC, futuro: outros)';
