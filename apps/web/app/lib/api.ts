import axios from "axios";
import type { AxiosRequestConfig } from "axios";
import { resolveBrowserApiBaseURL } from "./api-base-client.mjs";
import { isRequestCanceled as isRequestCanceledBase } from "./request-cancel.mjs";

/** Read double-submit CSRF cookie (tm_csrf / tm_csrf_hom / …). */
export function readCsrfToken(): string | null {
  if (typeof document === "undefined") return null;
  const parts = document.cookie.split(";").map((p) => p.trim());
  for (const part of parts) {
    const eq = part.indexOf("=");
    if (eq <= 0) continue;
    const name = part.slice(0, eq).trim();
    if (name === "tm_csrf" || name.startsWith("tm_csrf_")) {
      return decodeURIComponent(part.slice(eq + 1));
    }
  }
  return null;
}

export const api = axios.create({
  baseURL: resolveBrowserApiBaseURL(),
  timeout: 30000,
  withCredentials: true,
});

api.interceptors.request.use((config) => {
  // Browser session is cookie-only — never send Authorization from localStorage.
  if (config.headers) {
    delete (config.headers as any).Authorization;
    delete (config.headers as any).authorization;
  }
  const method = (config.method || "get").toUpperCase();
  if (method !== "GET" && method !== "HEAD" && method !== "OPTIONS") {
    const csrf = readCsrfToken();
    if (csrf) {
      config.headers = config.headers || {};
      (config.headers as any)["X-CSRF-Token"] = csrf;
    }
  }
  return config;
});

// Intercept 401 responses to redirect to login
// Intercept 403 password_change_required to redirect to change-password
api.interceptors.response.use(
  (response: any) => response,
  (error: any) => {
    const status = error?.response?.status;
    const path = typeof window !== "undefined" ? window.location.pathname : "";
    const onAuthFlow =
      path.startsWith("/change-password") ||
      path.startsWith("/reset-password") ||
      path.startsWith("/forgot-password") ||
      path.startsWith("/security");
    const errCode =
      error?.response?.data?.error || error?.response?.data?.detail?.error || "";

    // Não expulsar o usuário no meio da troca/redefinição de senha / MFA setup.
    // 401 mfa_required precisa ser tratado na própria tela.
    if (
      status === 401 &&
      typeof window !== "undefined" &&
      !path.match(/^\/?$/) &&
      !onAuthFlow &&
      errCode !== "mfa_required"
    ) {
      window.location.href = "/";
    }
    if (
      status === 403 &&
      typeof window !== "undefined" &&
      (error?.response?.data?.error === "password_change_required" ||
        error?.response?.data?.detail?.error === "password_change_required") &&
      !path.startsWith("/change-password")
    ) {
      window.location.href = "/change-password";
    }
    return Promise.reject(error);
  }
);

/** No-op for browser cookie sessions (kept for call-site compatibility). */
export function setAuthToken(_token: string | null) {
  delete api.defaults.headers.common["Authorization"];
}

export async function apiGet(path: string, config?: AxiosRequestConfig) {
  const res = await api.get(path, config);
  return res.data;
}

export async function apiPost(path: string, body: any, config?: AxiosRequestConfig) {
  const res = await api.post(path, body, config);
  return res.data;
}

export async function apiPatch(path: string, body: any, config?: AxiosRequestConfig) {
  const res = await api.patch(path, body, config);
  return res.data;
}

export async function apiPut(path: string, body: any, config?: AxiosRequestConfig) {
  const res = await api.put(path, body, config);
  return res.data;
}

export async function apiDelete(path: string, config?: AxiosRequestConfig) {
  const res = await api.delete(path, config);
  return res.data;
}

export function isRequestCanceled(error: any): boolean {
  return isRequestCanceledBase(error);
}
