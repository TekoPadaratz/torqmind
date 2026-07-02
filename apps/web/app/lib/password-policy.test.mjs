import test from "node:test";
import assert from "node:assert/strict";
import {
  PASSWORD_RULES,
  PASSWORD_MIN_LENGTH,
  PASSWORD_MAX_LENGTH,
  evaluatePassword,
  validatePassword,
  isValidPassword,
} from "./password-policy.mjs";

test("strong password passes all rules", () => {
  assert.equal(isValidPassword("Abcdef1!"), true);
  assert.deepEqual(validatePassword("Abcdef1!"), []);
});

test("rejects passwords missing each requirement", () => {
  assert.equal(isValidPassword("abcdef1!"), false); // sem maiúscula
  assert.equal(isValidPassword("ABCDEF1!"), false); // sem minúscula
  assert.equal(isValidPassword("Abcdefg!"), false); // sem número
  assert.equal(isValidPassword("Abcdefg1"), false); // sem especial
  assert.equal(isValidPassword("Ab1!"), false); // curta
});

test("validatePassword lists unmet rules", () => {
  const errors = validatePassword("abc");
  assert.ok(errors.length >= 3);
});

test("enforces max length", () => {
  const tooLong = "A1!" + "a".repeat(PASSWORD_MAX_LENGTH);
  assert.ok(validatePassword(tooLong).some((m) => m.includes("máximo")));
});

test("min length constant is 8", () => {
  assert.equal(PASSWORD_MIN_LENGTH, 8);
});

test("evaluatePassword returns ok flags per rule", () => {
  const state = evaluatePassword("Abcdef1!");
  assert.equal(state.length, PASSWORD_RULES.length);
  assert.ok(state.every((r) => r.ok === true));
});

test("handles null/undefined safely", () => {
  assert.equal(isValidPassword(undefined), false);
  assert.equal(isValidPassword(null), false);
});
