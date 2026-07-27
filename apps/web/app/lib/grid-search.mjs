/**
 * Pure grid-search helpers (shared by hook + node:test).
 * Aceita vírgula/ponto/moeda pt-BR na busca de valores.
 */

export function normalizeTerm(value) {
  return String(value || '').trim().toLocaleLowerCase('pt-BR');
}

function stripMoneyNoise(value) {
  return String(value || '')
    .replace(/\u00a0/g, ' ')
    .replace(/r\$\s*/gi, '')
    .replace(/\s+/g, '')
    .trim();
}

export function numericSearchVariants(raw) {
  const cleaned = stripMoneyNoise(raw);
  if (!cleaned || !/\d/.test(cleaned)) return [];

  const out = new Set([cleaned]);
  const hasComma = cleaned.includes(',');
  const hasDot = cleaned.includes('.');

  let canonical = cleaned;
  if (hasComma && hasDot) {
    canonical = cleaned.replace(/\./g, '').replace(',', '.');
  } else if (hasComma) {
    canonical = cleaned.replace(',', '.');
  } else if (hasDot) {
    const parts = cleaned.split('.');
    if (parts.length > 2) {
      canonical = cleaned.replace(/\./g, '');
    } else if (parts.length === 2 && parts[1].length === 3 && parts[0].length >= 1) {
      out.add(cleaned.replace(/\./g, ''));
    }
  }

  const asFloat = Number(canonical);
  if (Number.isFinite(asFloat)) {
    out.add(String(asFloat));
    out.add(asFloat.toFixed(2));
    out.add(asFloat.toFixed(2).replace('.', ','));
    out.add(String(Math.round(asFloat * 100) / 100));
    if (Math.abs(asFloat - Math.round(asFloat)) < 1e-9) {
      out.add(String(Math.round(asFloat)));
    }
    try {
      out.add(asFloat.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
      out.add(asFloat.toLocaleString('pt-BR', { minimumFractionDigits: 0, maximumFractionDigits: 2 }));
    } catch {
      /* ignore */
    }
  }

  out.add(cleaned.replace(/\./g, ','));
  out.add(cleaned.replace(/,/g, '.'));

  return [...out].map((v) => normalizeTerm(v)).filter(Boolean);
}

function stringifyCell(value) {
  if (value == null || value === '') return '';
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) {
    return value.map(stringifyCell).filter(Boolean).join(' ');
  }
  if (typeof value === 'object') {
    try {
      return Object.values(value)
        .map(stringifyCell)
        .filter(Boolean)
        .join(' ');
    } catch {
      return '';
    }
  }
  return '';
}

function cellSearchBlob(value) {
  const base = normalizeTerm(stringifyCell(value));
  const parts = new Set(base ? [base] : []);
  if (typeof value === 'number' && Number.isFinite(value)) {
    for (const v of numericSearchVariants(String(value))) parts.add(v);
    try {
      for (const v of numericSearchVariants(
        value.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      )) {
        parts.add(v);
      }
    } catch {
      /* ignore */
    }
  } else if (typeof value === 'string' && /\d/.test(value)) {
    for (const v of numericSearchVariants(value)) parts.add(v);
  }
  return [...parts].filter(Boolean).join(' | ');
}

const DEFAULT_EXCLUDE = /^(id_.*|.*_id|token|password|hash|payload|raw|reasons|motivos_raw)$/i;

export function rowMatchesGridSearch(row, term, options = {}) {
  const needleRaw = normalizeTerm(term);
  if (!needleRaw) return true;

  const needles = new Set([needleRaw]);
  if (/\d/.test(needleRaw)) {
    for (const v of numericSearchVariants(needleRaw)) needles.add(v);
  }

  const exclude = options.excludeKeys || DEFAULT_EXCLUDE;
  const keys = options.keys || Object.keys(row || {});
  for (const key of keys) {
    if (exclude.test(key)) continue;
    const hay = cellSearchBlob(row[key]);
    if (!hay) continue;
    for (const needle of needles) {
      if (needle && hay.includes(needle)) return true;
    }
  }
  return false;
}
