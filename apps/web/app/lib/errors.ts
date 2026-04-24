export function extractApiError(err: any, fallback = 'Falha na requisição'): string {
  const data = err?.response?.data;
  const detail = data?.detail;

  if (typeof detail?.message === 'string' && detail.message) {
    return detail.message;
  }

  if (typeof data?.error === 'string' && data.error) {
    if (typeof detail === 'string' && detail && detail !== data.error) {
      return `${data.error}: ${detail}`;
    }
    return data.error;
  }

  if (typeof detail === 'string') return detail;
  if (Array.isArray(detail)) {
    const parts = detail
      .map((item) => {
        if (typeof item?.msg === 'string') return item.msg;
        try {
          return JSON.stringify(item);
        } catch {
          return '';
        }
      })
      .filter(Boolean);
    return parts.length ? parts.join('; ') : fallback;
  }

  if (detail && typeof detail === 'object') {
    try {
      return JSON.stringify(detail);
    } catch {
      return fallback;
    }
  }

  if (typeof err?.message === 'string' && err.message) return err.message;
  return fallback;
}
