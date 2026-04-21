import assert from "node:assert/strict";
import test from "node:test";

import { USERNAME_ERROR_MESSAGE, normalizeUsernameInput, validateUsernameInput } from "./username-policy.mjs";

test("username policy normalizes lowercase and trims whitespace", () => {
  assert.equal(normalizeUsernameInput("  User.Name-01  "), "user.name-01");
});

test("username policy rejects invalid characters and accents", () => {
  assert.deepEqual(validateUsernameInput("João Silva"), {
    ok: false,
    normalized: "joão silva",
    message: USERNAME_ERROR_MESSAGE,
  });
});

test("username policy accepts valid normalized usernames", () => {
  assert.deepEqual(validateUsernameInput("tenant.owner_01"), {
    ok: true,
    normalized: "tenant.owner_01",
    message: null,
  });
});
