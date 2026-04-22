function padDatePart(value) {
  return String(value).padStart(2, '0');
}

function isLeapYear(year) {
  return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
}

function daysInMonth(year, month) {
  if (month === 2) return isLeapYear(year) ? 29 : 28;
  if ([4, 6, 9, 11].includes(month)) return 30;
  return 31;
}

export function formatCalendarDate(value) {
  if (!(value instanceof Date) || Number.isNaN(value.getTime())) return '';
  return `${value.getFullYear()}-${padDatePart(value.getMonth() + 1)}-${padDatePart(value.getDate())}`;
}

export function normalizeCalendarDate(value) {
  const raw = String(value || '').trim();
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return null;
  if (month < 1 || month > 12) return null;
  if (day < 1 || day > daysInMonth(year, month)) return null;

  return `${match[1]}-${match[2]}-${match[3]}`;
}

export function parseCalendarDate(value) {
  const normalized = normalizeCalendarDate(value);
  if (!normalized) return null;
  const [year, month, day] = normalized.split('-').map(Number);
  return new Date(year, month - 1, day);
}

export function addCalendarDays(date, amount) {
  const next = new Date(date);
  next.setDate(next.getDate() + amount);
  return next;
}

function startOfWeek(date) {
  const dayIndex = (date.getDay() + 6) % 7;
  return addCalendarDays(date, -dayIndex);
}

function startOfMonth(date) {
  return new Date(date.getFullYear(), date.getMonth(), 1);
}

function endOfMonth(date) {
  return new Date(date.getFullYear(), date.getMonth() + 1, 0);
}

export function buildQuickShortcutRanges(referenceDate) {
  const weekStart = startOfWeek(referenceDate);
  const lastWeekStart = addCalendarDays(weekStart, -7);
  const lastWeekEnd = addCalendarDays(weekStart, -1);
  const prevMonthDate = addCalendarDays(referenceDate, -referenceDate.getDate());
  const prevMonthStart = startOfMonth(prevMonthDate);
  const prevMonthEnd = endOfMonth(prevMonthDate);
  const fifteenDaysAgo = addCalendarDays(referenceDate, -14);
  const thirtyDaysAgo = addCalendarDays(referenceDate, -29);

  return [
    { id: 'today', label: 'Hoje', range: [formatCalendarDate(referenceDate), formatCalendarDate(referenceDate)] },
    { id: 'yesterday', label: 'Ontem', range: [formatCalendarDate(addCalendarDays(referenceDate, -1)), formatCalendarDate(addCalendarDays(referenceDate, -1))] },
    { id: 'this_week', label: 'Esta semana', range: [formatCalendarDate(weekStart), formatCalendarDate(referenceDate)] },
    { id: 'last_week', label: 'Semana passada', range: [formatCalendarDate(lastWeekStart), formatCalendarDate(lastWeekEnd)] },
    { id: 'last_15_days', label: '15 dias', range: [formatCalendarDate(fifteenDaysAgo), formatCalendarDate(referenceDate)] },
    { id: 'last_30_days', label: '30 dias', range: [formatCalendarDate(thirtyDaysAgo), formatCalendarDate(referenceDate)] },
    { id: 'last_90_days', label: '90 dias', range: [formatCalendarDate(addCalendarDays(referenceDate, -89)), formatCalendarDate(referenceDate)] },
    { id: 'this_month', label: 'Este mês', range: [formatCalendarDate(startOfMonth(referenceDate)), formatCalendarDate(referenceDate)] },
    { id: 'last_month', label: 'Mês passado', range: [formatCalendarDate(prevMonthStart), formatCalendarDate(prevMonthEnd)] },
  ];
}
