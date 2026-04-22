import assert from 'node:assert/strict';
import test from 'node:test';

import { buildBrowserLocalDefaultScope } from './local-scope-defaults.mjs';

test('browser local default scope keeps the browser calendar day even when server_today is ahead', () => {
  const scope = buildBrowserLocalDefaultScope({
    id_empresa: 7,
    id_filial: 11,
    server_today: '2026-04-16',
    default_scope: {
      id_empresa: 7,
      id_filial: 11,
      id_filiais: [11],
      days: 7,
      dt_ini: '2026-04-10',
      dt_fim: '2026-04-16',
      dt_ref: '2026-04-16',
      source: 'business_today_default',
    },
  });

  const localToday = new Date();
  const expectedDtFim = `${localToday.getFullYear()}-${String(localToday.getMonth() + 1).padStart(2, '0')}-${String(localToday.getDate()).padStart(2, '0')}`;

  assert.equal(scope.dt_fim, expectedDtFim);
  assert.equal(scope.dt_ref, expectedDtFim);
  assert.equal(scope.id_empresa, '7');
  assert.deepEqual(scope.id_filiais, ['11']);
  assert.equal(scope.source, 'browser_local_default');
});
