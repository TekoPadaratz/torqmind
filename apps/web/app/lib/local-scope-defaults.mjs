import { addCalendarDays, formatCalendarDate, parseCalendarDate } from './calendar-date.mjs';

function positiveInt(value, fallbackValue = 1) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallbackValue;
}

function normalizeBranchIds(values, fallbackValue = null) {
  if (Array.isArray(values) && values.length) {
    return [...new Set(values
      .map((value) => String(value).trim())
      .filter((value) => /^\d+$/.test(value) && Number(value) > 0))]
      .sort((left, right) => Number(left) - Number(right));
  }

  if (fallbackValue == null || String(fallbackValue).trim() === '') return [];
  const normalized = String(fallbackValue).trim();
  return /^\d+$/.test(normalized) && Number(normalized) > 0 ? [normalized] : [];
}

export function buildBrowserLocalDefaultScope(session) {
  const defaultScope = session?.default_scope || {};
  const localTodayDate = new Date();
  const localToday = formatCalendarDate(localTodayDate);
  const referenceDate = parseCalendarDate(localToday) || localTodayDate;
  const days = positiveInt(defaultScope?.days, 1);
  const startDate = addCalendarDays(referenceDate, -(days - 1));
  const branchIds = normalizeBranchIds(defaultScope?.id_filiais, defaultScope?.id_filial ?? session?.id_filial);

  return {
    ...defaultScope,
    id_empresa:
      defaultScope?.id_empresa != null && String(defaultScope.id_empresa).trim() !== ''
        ? String(defaultScope.id_empresa)
        : session?.id_empresa != null
          ? String(session.id_empresa)
          : null,
    id_filial:
      defaultScope?.id_filial != null && String(defaultScope.id_filial).trim() !== ''
        ? String(defaultScope.id_filial)
        : branchIds.length === 1
          ? branchIds[0]
          : session?.id_filial != null
            ? String(session.id_filial)
            : null,
    id_filiais: branchIds,
    dt_ini: formatCalendarDate(startDate),
    dt_fim: localToday,
    dt_ref: localToday,
    days,
    source: 'browser_local_default',
    browser_today: localToday,
  };
}
