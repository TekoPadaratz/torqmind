import assert from "node:assert/strict";
import test from "node:test";

import { resolveBrowserApiBaseURL } from "./api-base-client.mjs";
import { LOGIN_FORM_DEFAULTS } from "./login-form-defaults.mjs";
import { resolveServerApiBaseURL } from "./api-base-server.mjs";

test("browser always uses same-origin /api", () => {
  assert.equal(resolveBrowserApiBaseURL(), "/api");
  assert.equal(resolveBrowserApiBaseURL("/api"), "/api");
  assert.equal(resolveBrowserApiBaseURL("http://redevr.ddns.me:8000"), "/api");
});

test("server-side uses API_INTERNAL_URL and keeps internal port private", () => {
  assert.equal(resolveServerApiBaseURL("http://api:8000"), "http://api:8000");
  assert.equal(resolveServerApiBaseURL(""), "http://api:8000");
});

test("login form defaults stay empty for browser-native autofill only", () => {
  assert.deepEqual(LOGIN_FORM_DEFAULTS, { email: "", password: "" });
});
