import { api, setAuthToken } from './api';

/** Legacy JWT storage — cleared on load; never used for browser auth after Prompt 7. */
const TOKEN_KEY = 'torqmind.token';
const CLAIMS_KEY = 'torqmind.claims';
/** Soft flag only (no JWT). Cookie HttpOnly is the real session. */
const SESSION_FLAG_KEY = 'torqmind.session';

function purgeLegacyBearer(): void {
  if (typeof window === 'undefined') return;
  try {
    localStorage.removeItem(TOKEN_KEY);
    sessionStorage.removeItem('torqmind.mfa_setup');
  } catch {
    /* no-op */
  }
  setAuthToken(null);
}

/** Call once on app boot / auth module load. */
export function purgeLegacySessionArtifacts(): void {
  purgeLegacyBearer();
}

export function hasSession(): boolean {
  if (typeof window === 'undefined') return false;
  purgeLegacyBearer();
  return localStorage.getItem(SESSION_FLAG_KEY) === '1';
}

/** @deprecated Prefer hasSession(); browsers no longer hold JWTs. */
export function getToken(): string | null {
  purgeLegacyBearer();
  return null;
}

/** Mark soft session; JWT (if any) is ignored — cookies carry auth. */
export function setToken(_token?: string | null) {
  purgeLegacyBearer();
  if (typeof window !== 'undefined') {
    localStorage.setItem(SESSION_FLAG_KEY, '1');
  }
  setAuthToken(null);
}

export function markSession() {
  setToken(null);
}

export function clearToken() {
  if (typeof window !== 'undefined') {
    localStorage.removeItem(SESSION_FLAG_KEY);
    localStorage.removeItem(TOKEN_KEY);
  }
  setAuthToken(null);
}

export function getClaims(): any | null {
  if (typeof window === 'undefined') return null;
  const raw = localStorage.getItem(CLAIMS_KEY);
  return raw ? JSON.parse(raw) : null;
}

export function setClaims(claims: any) {
  localStorage.setItem(CLAIMS_KEY, JSON.stringify(claims));
}

export function clearClaims() {
  localStorage.removeItem(CLAIMS_KEY);
}

export function requireAuth(): boolean {
  purgeLegacyBearer();
  if (!hasSession()) {
    clearClaims();
    return false;
  }
  return true;
}

export function clearLocalAuth() {
  clearToken();
  clearClaims();
}

export function clearAuth() {
  clearLocalAuth();
  // Best-effort cookie clear (HttpOnly logout).
  if (typeof window !== 'undefined') {
    void api.post('/auth/logout').catch(() => undefined);
  }
}

// Purge any leftover bearer on module evaluation in the browser.
if (typeof window !== 'undefined') {
  purgeLegacyBearer();
}
