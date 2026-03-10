import axios from "axios";

function resolveBaseURL() {
  if (process.env.NEXT_PUBLIC_API_URL) return process.env.NEXT_PUBLIC_API_URL;
  if (process.env.NEXT_PUBLIC_API_BASE) return process.env.NEXT_PUBLIC_API_BASE;

  if (typeof window !== 'undefined') {
    return `${window.location.protocol}//${window.location.hostname}:8000`;
  }

  return 'http://192.168.0.125:8000';
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
