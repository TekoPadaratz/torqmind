import test from 'node:test';
import assert from 'node:assert/strict';

import { resolvePricingOverviewRequest } from './pricing-request.mjs';

test('resolvePricingOverviewRequest keeps a single deterministic pricing URL', () => {
  const result = resolvePricingOverviewRequest({
    dt_ini: '2026-03-20',
    dt_fim: '2026-03-29',
    id_empresa: '1',
    id_filial: '10169',
    scope_epoch: 'epoch-1',
  });

  assert.equal(result.error, null);
  assert.equal(
    result.requestUrl,
    '/bi/pricing/competitor/overview?dt_ini=2026-03-20&dt_fim=2026-03-29&id_empresa=1&id_filial=10169&scope_epoch=epoch-1&days_simulation=10',
  );
});

test('resolvePricingOverviewRequest rejects multiple branches', () => {
  const result = resolvePricingOverviewRequest({
    dt_ini: '2026-03-20',
    dt_fim: '2026-03-29',
    id_empresa: '1',
    id_filiais: ['10169', '14458'],
  });

  assert.equal(result.requestUrl, null);
  assert.match(result.error || '', /apenas uma filial/i);
});

test('resolvePricingOverviewRequest falls back to session branch when scope branch is absent', () => {
  const result = resolvePricingOverviewRequest(
    {
      dt_ini: '2026-03-20',
      dt_fim: '2026-03-29',
      id_empresa: '1',
      scope_epoch: 'epoch-2',
    },
    {
      id_empresa: 1,
      id_filial: 10169,
    },
  );

  assert.equal(result.error, null);
  assert.match(result.requestUrl || '', /id_filial=10169/);
});
