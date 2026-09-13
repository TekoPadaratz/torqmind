/**
 * Paginação, ordenação tipo Excel e totais de grids de registros.
 * Contrato: `.cursor/rules/08-grids-colunas-ordenacao.mdc`
 */
export const RECORD_PAGE_SIZE = 30;

export function pageRange(page, pageSize, total) {
  const safeTotal = Math.max(0, Number(total) || 0);
  const size = Math.max(1, Number(pageSize) || RECORD_PAGE_SIZE);
  if (safeTotal === 0) return { from: 0, to: 0 };
  const safePage = clampPage(page, safeTotal, size);
  const from = (safePage - 1) * size + 1;
  const to = Math.min(safePage * size, safeTotal);
  return { from, to, page: safePage };
}

export function clampPage(page, total, pageSize) {
  const size = Math.max(1, Number(pageSize) || RECORD_PAGE_SIZE);
  const totalPages = Math.max(1, Math.ceil((Number(total) || 0) / size) || 1);
  const raw = Number(page) || 1;
  return Math.min(Math.max(1, raw), totalPages);
}

export function paginateRows(rows, page, pageSize = RECORD_PAGE_SIZE) {
  const list = Array.isArray(rows) ? rows : [];
  const size = Math.max(1, Number(pageSize) || RECORD_PAGE_SIZE);
  const safe = clampPage(page, list.length, size);
  const start = (safe - 1) * size;
  return {
    page: safe,
    total: list.length,
    totalPages: Math.max(1, Math.ceil(list.length / size) || 1),
    slice: list.slice(start, start + size),
  };
}

function asNumber(value) {
  if (value == null || value === '') return null;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const raw = String(value).trim().replace(/\s/g, '');
  if (!raw) return null;
  const br = raw.replace(/R\$/i, '').replace(/\./g, '').replace(',', '.');
  const n = Number(br);
  return Number.isFinite(n) ? n : null;
}

function asTime(value) {
  if (value == null || value === '') return null;
  if (typeof value === 'number') {
    if (value > 19000101 && value < 21001231) return value;
    return value;
  }
  if (value instanceof Date) {
    const t = value.getTime();
    return Number.isNaN(t) ? null : t;
  }
  const raw = String(value).trim();
  if (/^\d{8}$/.test(raw)) return Number(raw);
  const t = Date.parse(raw);
  return Number.isNaN(t) ? null : t;
}

function asText(value) {
  return String(value ?? '')
    .trim()
    .toLocaleUpperCase('pt-BR');
}

/** nulls por último, em qualquer direção. */
export function compareTyped(a, b, type = 'text') {
  if (type === 'number') {
    const na = asNumber(a);
    const nb = asNumber(b);
    if (na == null && nb == null) return 0;
    if (na == null) return 1;
    if (nb == null) return -1;
    return na - nb;
  }
  if (type === 'date') {
    const da = asTime(a);
    const db = asTime(b);
    if (da == null && db == null) return 0;
    if (da == null) return 1;
    if (db == null) return -1;
    return da - db;
  }
  const ta = asText(a);
  const tb = asText(b);
  if (!ta && !tb) return 0;
  if (!ta) return 1;
  if (!tb) return -1;
  return ta.localeCompare(tb, 'pt-BR');
}

export function sortRecordRows(rows, spec) {
  const list = Array.isArray(rows) ? [...rows] : [];
  const key = spec?.key;
  const type = spec?.type || 'text';
  const dir = spec?.dir === 'desc' ? -1 : 1;
  const getValue = spec?.getValue || ((row) => row?.[key]);
  const getTieId = spec?.getTieId || ((row, idx) => row?.id ?? idx);
  list.sort((left, right) => {
    const cmp = compareTyped(getValue(left), getValue(right), type);
    if (cmp !== 0) return cmp * dir;
    const ia = getTieId(left, 0);
    const ib = getTieId(right, 0);
    if (typeof ia === 'number' && typeof ib === 'number') {
      return ia - ib;
    }
    const sa = String(ia ?? '');
    const sb = String(ib ?? '');
    if (sa !== sb) return sa.localeCompare(sb, 'pt-BR');
    return 0;
  });
  return list;
}

export function sumFields(rows, keys) {
  const out = {};
  for (const key of keys || []) {
    out[key] = 0;
  }
  for (const row of rows || []) {
    for (const key of keys || []) {
      const n = asNumber(row?.[key]);
      if (n != null) out[key] += n;
    }
  }
  for (const key of keys || []) {
    out[key] = Math.round(out[key] * 100) / 100;
  }
  return out;
}

export function nextSortDir(currentKey, currentDir, nextKey, defaultDir = 'asc') {
  if (currentKey !== nextKey) return defaultDir;
  return currentDir === 'asc' ? 'desc' : 'asc';
}
