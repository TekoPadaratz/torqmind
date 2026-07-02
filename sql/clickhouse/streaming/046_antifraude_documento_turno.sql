-- ============================================================
-- Antifraude: documento operacional + turno operacional real
-- ============================================================
-- Adiciona ao mart_antifraude_eventos:
--   id_comprovante  : PK tecnico do comprovante (rastreabilidade no caixa).
--   nro_comprovante : NROCOMPROVANTE (numero impresso no comprovante de venda).
--   turno_numero    : turno OPERACIONAL real (1..N; 0 = caixa geral),
--                     extraido de stg_turnos.payload.TURNO.
--
-- Regra TorqMind: NUNCA exibir id_turno tecnico (ID_TURNOS) como numero de
-- turno. O turno operacional e geralmente 1..5; turno 0 e caixa geral.
-- Documento preferencial e o numero do comprovante de venda (NROCOMPROVANTE);
-- fallback e o id_comprovante. NUNCA "Turno + Filial" como documento.
--
-- Idempotente: ADD COLUMN IF NOT EXISTS. Linhas existentes ganham default 0
-- ate o proximo refresh/backfill repopular com o valor real.
-- ============================================================

ALTER TABLE torqmind_mart_rt.mart_antifraude_eventos
    ADD COLUMN IF NOT EXISTS id_comprovante  Int32 DEFAULT 0;

ALTER TABLE torqmind_mart_rt.mart_antifraude_eventos
    ADD COLUMN IF NOT EXISTS nro_comprovante Int64 DEFAULT 0;

ALTER TABLE torqmind_mart_rt.mart_antifraude_eventos
    ADD COLUMN IF NOT EXISTS turno_numero    Int32 DEFAULT 0;
