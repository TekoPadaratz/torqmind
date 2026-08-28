/** Período de comissão 21→20 e cache de sessão (Metas → Comissões). */

const SESSION_KEY = 'torqmind.goals.commissionPeriod';

function pad2(n) {
  return String(n).padStart(2, '0');
}

export function formatIsoDate(d) {
  return `${d.year}-${pad2(d.month)}-${pad2(d.day)}`;
}

export function parseIsoDate(value) {
  const text = String(value || '').trim().slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return null;
  const [y, m, d] = text.split('-').map(Number);
  if (!y || !m || !d) return null;
  return { year: y, month: m, day: d };
}

function addMonths(year, month, delta) {
  const idx = month - 1 + delta;
  return { year: year + Math.floor(idx / 12), month: (idx % 12) + 1 };
}

/** Último ciclo 21→20 fechado (calendário local do browser). */
export function defaultLastClosedCommissionPeriod(ref = new Date()) {
  const y = ref.getFullYear();
  const m = ref.getMonth() + 1;
  const d = ref.getDate();
  if (d >= 21) {
    const start = addMonths(y, m, -1);
    return {
      dt_ini: formatIsoDate({ ...start, day: 21 }),
      dt_fim: formatIsoDate({ year: y, month: m, day: 20 }),
    };
  }
  const end = addMonths(y, m, -1);
  const start = addMonths(y, m, -2);
  return {
    dt_ini: formatIsoDate({ ...start, day: 21 }),
    dt_fim: formatIsoDate({ ...end, day: 20 }),
  };
}

export function validateCommissionPeriod(dtIni, dtFim) {
  const a = parseIsoDate(dtIni);
  const b = parseIsoDate(dtFim);
  if (!a || !b) return 'Informe datas válidas (AAAA-MM-DD).';
  const t1 = Date.UTC(a.year, a.month - 1, a.day);
  const t2 = Date.UTC(b.year, b.month - 1, b.day);
  if (t2 < t1) return 'A data final deve ser igual ou posterior à data inicial.';
  return null;
}

export function readCommissionPeriodSession() {
  if (typeof window === 'undefined') return defaultLastClosedCommissionPeriod();
  try {
    const raw = sessionStorage.getItem(SESSION_KEY);
    if (!raw) return defaultLastClosedCommissionPeriod();
    const parsed = JSON.parse(raw);
    const dt_ini = String(parsed?.dt_ini || '').slice(0, 10);
    const dt_fim = String(parsed?.dt_fim || '').slice(0, 10);
    if (validateCommissionPeriod(dt_ini, dt_fim)) return defaultLastClosedCommissionPeriod();
    return { dt_ini, dt_fim };
  } catch {
    return defaultLastClosedCommissionPeriod();
  }
}

export function persistCommissionPeriodSession(period) {
  try {
    sessionStorage.setItem(SESSION_KEY, JSON.stringify(period));
  } catch {
    /* quota / private mode */
  }
}

export function formatCommissionPeriodLabel(dtIni, dtFim) {
  const a = parseIsoDate(dtIni);
  const b = parseIsoDate(dtFim);
  if (!a || !b) return '—';
  const fmt = (p) => `${pad2(p.day)}/${pad2(p.month)}/${p.year}`;
  return `${fmt(a)} – ${fmt(b)}`;
}
