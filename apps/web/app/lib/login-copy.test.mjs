import assert from "node:assert/strict";
import test from "node:test";

import { LOGIN_IDENTIFIER_LABEL, LOGIN_IDENTIFIER_PLACEHOLDER } from "./login-copy.mjs";

test("login copy clearly accepts username or email", () => {
  assert.equal(LOGIN_IDENTIFIER_LABEL, "Nome de usuário ou e-mail");
  assert.equal(LOGIN_IDENTIFIER_PLACEHOLDER, "Nome de usuário ou e-mail");
});
