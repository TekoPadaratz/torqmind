import { apiGet } from './api';
import { clearAuth, getClaims, requireAuth, setClaims } from './auth';
import { isConfirmedSessionInvalidation } from './session-state.mjs';

let sessionCache: any | null = null;
let sessionPromise: Promise<any> | null = null;

export function readCachedSession(): any | null {
  return sessionCache || getClaims();
}

export function cacheSession(session: any | null) {
  if (!session) return null;
  sessionCache = session;
  setClaims(session);
  return sessionCache;
}

export function clearSessionCache() {
  sessionCache = null;
  sessionPromise = null;
}

export async function fetchSession(force = false) {
  if (!force) {
    if (sessionCache) return sessionCache;
    if (sessionPromise) return sessionPromise;
  }

  sessionPromise = apiGet('/auth/me')
    .then((me) => {
      return cacheSession(me);
    })
    .finally(() => {
      sessionPromise = null;
    });

  return sessionPromise;
}

export async function loadSession(router: any, area: 'product' | 'platform', options?: { force?: boolean }) {
  if (!requireAuth()) {
    clearSessionCache();
    router.push('/');
    return null;
  }

  try {
    const me = await fetchSession(Boolean(options?.force));

    const canUseProduct = Boolean(me?.access?.product);
    const canUsePlatform = Boolean(me?.access?.platform);

    if (area === 'product' && !canUseProduct) {
      router.push(me?.home_path || '/platform');
      return null;
    }
    if (area === 'platform' && !canUsePlatform) {
      router.push(me?.home_path || '/dashboard');
      return null;
    }

    return me;
  } catch (error: any) {
    if (isConfirmedSessionInvalidation(error)) {
      clearSessionCache();
      clearAuth();
      router.push('/');
      return null;
    }

    const cached = sessionCache || getClaims();
    if (cached) return cached;
    throw error;
  }
}

export function isPlatformFinance(me: any): boolean {
  return Boolean(me?.access?.platform_finance);
}

export function isPlatformOps(me: any): boolean {
  return Boolean(me?.access?.platform_operations);
}
