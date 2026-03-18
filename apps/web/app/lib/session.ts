import { apiGet } from './api';
import { clearAuth, requireAuth, setClaims } from './auth';

export async function loadSession(router: any, area: 'product' | 'platform') {
  if (!requireAuth()) {
    router.push('/');
    return null;
  }

  try {
    const me = await apiGet('/auth/me');
    setClaims(me);

    const canUseProduct = Boolean(me?.access?.product);
    const canUsePlatform = Boolean(me?.access?.platform);

    if (area === 'product' && !canUseProduct) {
      router.push(me?.home_path || '/platform');
      return null;
    }
    if (area === 'platform' && !canUsePlatform) {
      router.push(me?.home_path || '/scope');
      return null;
    }

    return me;
  } catch {
    clearAuth();
    router.push('/');
    return null;
  }
}

export function isPlatformFinance(me: any): boolean {
  return Boolean(me?.access?.platform_finance);
}

export function isPlatformOps(me: any): boolean {
  return Boolean(me?.access?.platform_operations);
}
