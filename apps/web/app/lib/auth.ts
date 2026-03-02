import { setAuthToken } from './api';

const TOKEN_KEY = 'torqmind.token';
const CLAIMS_KEY = 'torqmind.claims';

export function getToken(): string | null {
  if (typeof window === 'undefined') return null;
  return localStorage.getItem(TOKEN_KEY);
}

export function setToken(token: string) {
  localStorage.setItem(TOKEN_KEY, token);
  setAuthToken(token);
}

export function clearToken() {
  localStorage.removeItem(TOKEN_KEY);
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
  const t = getToken();
  if (!t) return false;
  setAuthToken(t);
  return true;
}

export function clearAuth() {
  clearToken();
  clearClaims();
}
