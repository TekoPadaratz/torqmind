/**
 * Ordenação canônica de grids BI.
 * Contrato: `.cursor/rules/08-grids-colunas-ordenacao.mdc`
 * Prioridade: Filial ASC → Data DESC → Nome ASC.
 */

function filialKey(value) {
  if (value == null || value === '') return '';
  return String(value).trim().toLocaleUpperCase('pt-BR');
}

function dataKey(value) {
  if (value == null || value === '') return 0;
  if (typeof value === 'number') {
    if (value > 19000101 && value < 21001231) return value;
    return value;
  }
  if (value instanceof Date) {
    const t = value.getTime();
    return Number.isNaN(t) ? 0 : t;
  }
  const raw = String(value).trim();
  if (/^\d{8}$/.test(raw)) return Number(raw);
  const t = Date.parse(raw);
  return Number.isNaN(t) ? 0 : t;
}

function nomeKey(value) {
  return String(value || '')
    .trim()
    .toLocaleUpperCase('pt-BR');
}

/** Comparador estável Filial ASC → Data DESC → Nome ASC. */
export function compareGridRows(a, b) {
  const fa = filialKey(a?.filial);
  const fb = filialKey(b?.filial);
  if (fa !== fb) return fa < fb ? -1 : 1;

  const da = dataKey(a?.data);
  const db = dataKey(b?.data);
  if (da !== db) return db - da;

  const na = nomeKey(a?.nome);
  const nb = nomeKey(b?.nome);
  if (na !== nb) return na < nb ? -1 : 1;
  return 0;
}

export function sortGridRows(rows, pick) {
  return [...rows].sort((left, right) => compareGridRows(pick(left), pick(right)));
}
