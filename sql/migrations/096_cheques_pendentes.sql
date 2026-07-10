-- Migration 096: Controle de cheques recebidos NAO compensados (Financeiro)
-- Idempotente. Nao destrutivo.
--
-- Objetivo (linguagem do cliente): mostrar no Financeiro os cheques recebidos
-- que ainda NAO foram compensados (nao caiu o dinheiro), com um card do total
-- ja VENCIDO (data "bom para" passou) e uma lista completa por posto.
--
-- Fonte canonica (Xpert): dbo.CHEQUESRECEBIDOS. "Nao compensado" = DTACOMPENSADO
-- vazio. "Vencido" = DTABOM < hoje. Esta tabela e a camada RAPIDA da tela: em
-- producao e populada pelo Agent (dataset cheques) -> STG -> refresh; em
-- homologacao e semeada com amostra real do Xpert. Cheques compensados saem
-- da tabela no proximo refresh (so guardamos os pendentes).

CREATE TABLE IF NOT EXISTS mart.cheques_pendentes (
  id_empresa      integer      NOT NULL,
  id_filial       integer      NOT NULL,
  id_db           integer      NOT NULL,
  id_cheque       integer      NOT NULL,   -- ID_CHEQUESRECEBIDOS
  id_entidade     integer      NULL,       -- cliente (rastreabilidade)
  cliente_nome    text         NOT NULL DEFAULT '',
  cpf             text         NOT NULL DEFAULT '',
  valor           numeric(18,2) NOT NULL DEFAULT 0,
  dt_recebido     date         NULL,       -- DTACONTA
  dt_vencimento   date         NULL,       -- DTABOM (bom para / vencimento)
  situacao_cheque smallint     NULL,       -- SITUACAOCHEQUE (rastreabilidade)
  banco           text         NOT NULL DEFAULT '',
  agencia         text         NOT NULL DEFAULT '',
  nroconta        text         NOT NULL DEFAULT '',
  numero          text         NOT NULL DEFAULT '',
  updated_at      timestamptz  NOT NULL DEFAULT now(),
  PRIMARY KEY (id_empresa, id_filial, id_db, id_cheque)
);

CREATE INDEX IF NOT EXISTS ix_cheques_pendentes_scope
  ON mart.cheques_pendentes (id_empresa, id_filial, dt_vencimento);

COMMENT ON TABLE mart.cheques_pendentes IS
  'Cheques recebidos ainda NAO compensados (camada rapida do Financeiro). Fonte: dbo.CHEQUESRECEBIDOS (DTACOMPENSADO vazio). Vencido = dt_vencimento < hoje.';
