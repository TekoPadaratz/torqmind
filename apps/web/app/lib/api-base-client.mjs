const DEFAULT_BROWSER_API_BASE_URL = "/api";

function normalizeBaseURL(value) {
  if (!value) return "";
  return value.replace(/\/+$/, "");
}

export function resolveBrowserApiBaseURL(publicBaseURL = process.env.NEXT_PUBLIC_API_BASE_URL) {
  const normalized = normalizeBaseURL(publicBaseURL || DEFAULT_BROWSER_API_BASE_URL);
  return normalized === DEFAULT_BROWSER_API_BASE_URL ? normalized : DEFAULT_BROWSER_API_BASE_URL;
}
