-- Migration 095: apelido curto por filial (exibido em todo o sistema)
-- Idempotente. Nao destrutivo. Nao apaga nada.
--
-- Objetivo (linguagem do cliente): dar um "apelido" curto para cada posto
-- (ex.: "VR 01") no lugar do nome completo do cadastro ("AUTO POSTO VR 01 LTDA").
-- O apelido e editado na tela Plataforma > Empresa > Filiais e passa a aparecer
-- em todas as telas, rankings e alertas. Vazio = usa o nome completo.

ALTER TABLE auth.filiais
  ADD COLUMN IF NOT EXISTS apelido text;

COMMENT ON COLUMN auth.filiais.apelido IS
  'Apelido curto da filial (ex.: "VR 01"), definido na Plataforma. Quando preenchido, substitui o nome completo em toda a interface e alertas. Vazio/NULL = usa o nome.';

-- Backfill inicial da empresa 1 (rede Verenka). So preenche onde ainda esta
-- vazio, para nunca sobrescrever ajustes manuais feitos depois na Plataforma.
UPDATE auth.filiais AS f
SET apelido = v.apelido
FROM (VALUES
  (1, 14458, 'VR 01'),
  (1, 17337, 'VR 02'),
  (1, 14459, 'VR 03'),
  (1, 16305, 'VR 04'),
  (1, 14122, 'VR 05'),
  (1, 11621, 'VR 06'),
  (1, 10169, 'VR 07'),
  (1, 15383, 'VR 08'),
  (1, 15172, 'VR 09'),
  (1, 18096, 'VR 10'),
  (1, 18339, 'VR 11'),
  (1, 14126, 'Central Verenka')
) AS v(id_empresa, id_filial, apelido)
WHERE f.id_empresa = v.id_empresa
  AND f.id_filial = v.id_filial
  AND (f.apelido IS NULL OR btrim(f.apelido) = '');
