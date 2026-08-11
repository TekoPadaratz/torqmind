import { addCalendarDays, formatBusinessCalendarDate, formatCalendarDate, parseCalendarDate } from './calendar-date.mjs';

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

function deriveAccessibleBranchIds(session, tenantId) {
  if (tenantId == null || String(tenantId).trim() === '') return [];
  const normalizedTenantId = String(tenantId).trim();
  const accesses = Array.isArray(session?.accesses) ? session.accesses : [];

  return [...new Set(accesses
    .filter((access) => String(access?.id_empresa ?? '').trim() === normalizedTenantId)
    .map((access) => String(access?.id_filial ?? '').trim())
    .filter((value) => /^\d+$/.test(value) && Number(value) > 0))]
    .sort((left, right) => Number(left) - Number(right));
}

export function buildBrowserLocalDefaultScope(session) {
  const defaultScope = session?.default_scope || {};
  const localTodayDate = new Date();
  const localToday = formatBusinessCalendarDate(localTodayDate);
  const referenceDate = parseCalendarDate(localToday) || localTodayDate;
  const days = positiveInt(defaultScope?.days, 30);
  const startDate = addCalendarDays(referenceDate, -(days - 1));
  const productCompanyId = Array.isArray(session?.product_companies)
    ? session.product_companies
      .map((row) => row?.id_empresa)
      .find((value) => value != null && String(value).trim() !== '' && Number(value) > 0)
    : null;
  const id_empresa =
    defaultScope?.id_empresa != null && String(defaultScope.id_empresa).trim() !== ''
      ? String(defaultScope.id_empresa)
      : session?.id_empresa != null
        ? String(session.id_empresa)
        : Array.isArray(session?.tenant_ids) && session.tenant_ids.length
          ? String(session.tenant_ids[0])
          : productCompanyId != null
            ? String(productCompanyId)
            : null;
  const explicitBranchIds = normalizeBranchIds(defaultScope?.id_filiais, defaultScope?.id_filial ?? session?.id_filial);
  const branchIds = explicitBranchIds.length ? explicitBranchIds : deriveAccessibleBranchIds(session, id_empresa);
  // Sem filiais explícitas: "all" torna a URL canônica (needsCanonical exige branch).
  const branchScope = String(defaultScope?.branch_scope || '').trim().toLowerCase()
    || (explicitBranchIds.length === 0 && branchIds.length > 1 ? 'all' : '')
    || (id_empresa && explicitBranchIds.length === 0 && branchIds.length === 0 ? 'all' : '');

  return {
    ...defaultScope,
    id_empresa,
    id_filial:
      defaultScope?.id_filial != null && String(defaultScope.id_filial).trim() !== ''
        ? String(defaultScope.id_filial)
        : branchIds.length === 1
          ? branchIds[0]
          : session?.id_filial != null
            ? String(session.id_filial)
            : null,
    id_filiais: branchIds,
    branch_scope: branchScope,
    dt_ini: formatCalendarDate(startDate),
    dt_fim: localToday,
    dt_ref: localToday,
    days,
    source: 'browser_local_default',
    browser_today: localToday,
  };
}
