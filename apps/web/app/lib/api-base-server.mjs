const DEFAULT_SERVER_API_BASE_URL = "http://api:8000";

function normalizeBaseURL(value) {
  if (!value) return "";
  return value.replace(/\/+$/, "");
}

export function resolveServerApiBaseURL(internalBaseURL = process.env.API_INTERNAL_URL) {
  return normalizeBaseURL(internalBaseURL || DEFAULT_SERVER_API_BASE_URL);
}
