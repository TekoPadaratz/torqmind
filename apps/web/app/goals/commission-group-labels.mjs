/**
 * Rótulos de grupo na configuração de comissões (código = id_grupo_produto no Xpert).
 */

/** Ex.: "5 — LUBRIFICANTES" */
export function formatGroupLabel(idGrupoProduto, nome) {
  const id = Number(idGrupoProduto);
  const safeId = Number.isFinite(id) && id > 0 ? id : 0;
  const label = String(nome || "").trim() || (safeId ? `Grupo ${safeId}` : "Grupo");
  return safeId ? `${safeId} — ${label}` : label;
}

/** Lista só os códigos dos grupos marcados, ordenados (ex.: "5, 6, 11, 12"). */
export function formatSelectedGroupCodes(groups) {
  const ids = (groups || [])
    .filter((g) => g && g.selected)
    .map((g) => Number(g.id_grupo_produto))
    .filter((id) => Number.isFinite(id) && id > 0)
    .sort((a, b) => a - b);
  if (!ids.length) return "nenhum";
  return ids.join(", ");
}
