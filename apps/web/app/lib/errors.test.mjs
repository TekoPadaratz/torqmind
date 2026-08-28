import test from 'node:test';
import assert from 'node:assert/strict';
import { coerceDisplayMessage, extractApiError } from './errors.mjs';

test('extractApiError lê detail.message de objeto FastAPI', () => {
  const err = {
    response: {
      data: {
        detail: {
          error: 'screen_access_denied',
          message: 'Acesso negado à tela.',
        },
      },
    },
  };
  assert.equal(extractApiError(err, 'fallback'), 'Acesso negado à tela.');
});

test('coerceDisplayMessage evita objeto cru em JSX', () => {
  assert.equal(coerceDisplayMessage({ message: 'Sem vendas' }), 'Sem vendas');
  assert.equal(coerceDisplayMessage('ok'), 'ok');
  assert.equal(coerceDisplayMessage(null, 'fb'), 'fb');
});
