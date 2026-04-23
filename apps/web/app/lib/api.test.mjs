import test from "node:test";
import assert from "node:assert/strict";

import { isRequestCanceled } from "./request-cancel.mjs";

test("isRequestCanceled recognizes axios and browser abort signatures", () => {
  assert.equal(isRequestCanceled({ code: "ERR_CANCELED", message: "canceled" }), true);
  assert.equal(isRequestCanceled({ name: "CanceledError", message: "canceled" }), true);
  assert.equal(isRequestCanceled({ name: "AbortError", message: "The operation was aborted." }), true);
  assert.equal(isRequestCanceled({ code: "ECONNABORTED", message: "timeout of 30000ms exceeded" }), false);
  assert.equal(isRequestCanceled(new Error("other failure")), false);
});
