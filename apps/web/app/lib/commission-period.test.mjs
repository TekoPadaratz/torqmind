import assert from 'node:assert/strict';
import test from 'node:test';

import {
  defaultLastClosedCommissionPeriod,
  formatCommissionPeriodLabel,
  validateCommissionPeriod,
} from './commission-period.mjs';

test('default period on 2025-08-15 is 21/06–20/07', () => {
  const ref = new Date(2025, 7, 15);
  const p = defaultLastClosedCommissionPeriod(ref);
  assert.equal(p.dt_ini, '2025-06-21');
  assert.equal(p.dt_fim, '2025-07-20');
});

test('default period on 2025-08-28 is 21/07–20/08', () => {
  const ref = new Date(2025, 7, 28);
  const p = defaultLastClosedCommissionPeriod(ref);
  assert.equal(p.dt_ini, '2025-07-21');
  assert.equal(p.dt_fim, '2025-08-20');
});

test('default period on 2025-08-21 is 21/07–20/08', () => {
  const ref = new Date(2025, 7, 21);
  const p = defaultLastClosedCommissionPeriod(ref);
  assert.equal(p.dt_ini, '2025-07-21');
  assert.equal(p.dt_fim, '2025-08-20');
});

test('validate rejects inverted range', () => {
  assert.ok(validateCommissionPeriod('2025-08-20', '2025-08-01'));
});

test('format period label', () => {
  assert.equal(
    formatCommissionPeriodLabel('2025-07-21', '2025-08-20'),
    '21/07/2025 – 20/08/2025',
  );
});
