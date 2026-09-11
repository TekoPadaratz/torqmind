import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { test } from "node:test";

const require = createRequire(import.meta.url);

test("react-is is pinned to 19.x and recharts to 2.15.x", () => {
  const reactIsPkg = require("react-is/package.json");
  const rechartsPkg = require("recharts/package.json");

  assert.ok(
    String(reactIsPkg.version).startsWith("19"),
    `expected react-is 19.x, got ${reactIsPkg.version}`,
  );
  assert.ok(
    String(rechartsPkg.version).startsWith("2.15"),
    `expected recharts 2.15.x, got ${rechartsPkg.version}`,
  );

  // Smoke: react-is 19 exports isValidElementType for consumers like recharts.
  const reactIs = require("react-is");
  assert.equal(typeof reactIs.isValidElementType, "function");
});
