import axios from "axios";
import type { AxiosRequestConfig } from "axios";
import { resolveBrowserApiBaseURL } from "./api-base-client.mjs";
import { isRequestCanceled as isRequestCanceledBase } from "./request-cancel.mjs";

export const api = axios.create({
  baseURL: resolveBrowserApiBaseURL(),
  timeout: 30000,
});

// Intercept 401 responses to redirect to login
// Intercept 403 password_change_required to redirect to change-password
api.interceptors.response.use(
  (response: any) => response,
  (error: any) => {
    const status = error?.response?.status;
    if (
      status === 401 &&
      typeof window !== "undefined" &&
      !window.location.pathname.match(/^\/?$/)
    ) {
      window.location.href = "/";
    }
    if (
      status === 403 &&
      typeof window !== "undefined" &&
      error?.response?.data?.error === "password_change_required" &&
      !window.location.pathname.startsWith("/change-password")
    ) {
      window.location.href = "/change-password";
    }
    return Promise.reject(error);
  }
);

export function setAuthToken(token: string | null) {
  if (token) {
    api.defaults.headers.common["Authorization"] = `Bearer ${token}`;
  } else {
    delete api.defaults.headers.common["Authorization"];
  }
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
