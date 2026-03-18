import axios from "axios";
import { resolveBrowserApiBaseURL } from "./api-base-client.mjs";

export const api = axios.create({
  baseURL: resolveBrowserApiBaseURL(),
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
