import assert from 'node:assert/strict';
import test from 'node:test';

import { isConfirmedSessionInvalidation } from './session-state.mjs';

test('session invalidation treats invalid tokens as confirmed auth failures', () => {
  assert.equal(
    isConfirmedSessionInvalidation({
      response: {
        status: 401,
        data: { detail: { error: 'invalid_token' } },
      },
    }),
    true,
  );
});

test('session invalidation treats access-unavailable responses as confirmed session loss', () => {
  assert.equal(
    isConfirmedSessionInvalidation({
      response: {
        status: 403,
        data: { detail: { error: 'access_unavailable' } },
      },
    }),
    true,
  );
});

test('session invalidation preserves auth on generic transient failures', () => {
  assert.equal(
    isConfirmedSessionInvalidation({
      response: {
        status: 503,
        data: { detail: { error: 'service_unavailable' } },
      },
    }),
    false,
  );
  assert.equal(isConfirmedSessionInvalidation(new Error('Network Error')), false);
});
