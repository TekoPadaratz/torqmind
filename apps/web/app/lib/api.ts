import axios from "axios";

const DEFAULT_API_PORT = process.env.NEXT_PUBLIC_API_PORT || "8000";

function normalizeBaseURL(value: string) {
  return value.replace(/\/+$/, "");
}

function resolveBaseURL() {
  if (process.env.NEXT_PUBLIC_API_URL) return normalizeBaseURL(process.env.NEXT_PUBLIC_API_URL);

  if (typeof window !== "undefined") {
    return `${window.location.protocol}//${window.location.hostname}:${DEFAULT_API_PORT}`;
  }

  if (process.env.API_INTERNAL_URL) return normalizeBaseURL(process.env.API_INTERNAL_URL);

  return `http://api:${DEFAULT_API_PORT}`;
}

export const api = axios.create({
  baseURL: resolveBaseURL(),
  timeout: 30000,
});

export function setAuthToken(token: string | null) {
  if (token) {
    api.defaults.headers.common["Authorization"] = `Bearer ${token}`;
  } else {
    delete api.defaults.headers.common["Authorization"];
  }
}

export async function apiGet(path: string) {
  const res = await api.get(path);
  return res.data;
}

export async function apiPost(path: string, body: any) {
  const res = await api.post(path, body);
  return res.data;
}
