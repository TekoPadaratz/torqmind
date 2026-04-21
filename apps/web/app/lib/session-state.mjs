function normalizeErrorCode(error) {
  const detail = error?.response?.data?.detail;
  if (typeof detail === 'string') return detail.trim().toLowerCase();
  if (detail && typeof detail === 'object') return String(detail.error || '').trim().toLowerCase();
  return '';
}

export function isConfirmedSessionInvalidation(error) {
  const status = Number(error?.response?.status || 0);
  const errorCode = normalizeErrorCode(error);

  if (status === 401) return true;
  if (status !== 403) return false;

  return new Set([
    'access_unavailable',
    'inactive_user',
    'invalid_token',
    'missing_bearer',
    'scope_forbidden',
    'user_disabled',
  ]).has(errorCode);
}
