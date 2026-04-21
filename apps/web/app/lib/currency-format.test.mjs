import assert from 'node:assert/strict';
import test from 'node:test';

import { formatCurrencyValue } from './currency-format.mjs';

function normalizeCurrencyOutput(value) {
  return String(value).replace(/\u00a0/g, ' ');
}

test('currency formatter renders integer values in BRL format', () => {
  assert.equal(normalizeCurrencyOutput(formatCurrencyValue(1000)), 'R$ 1.000,00');
});

test('currency formatter renders decimal values in BRL format', () => {
  assert.equal(normalizeCurrencyOutput(formatCurrencyValue(1000.5)), 'R$ 1.000,50');
});

test('currency formatter renders zero in BRL format', () => {
  assert.equal(normalizeCurrencyOutput(formatCurrencyValue(0)), 'R$ 0,00');
});

test('currency formatter normalizes null and undefined to zero', () => {
  assert.equal(normalizeCurrencyOutput(formatCurrencyValue(null)), 'R$ 0,00');
  assert.equal(normalizeCurrencyOutput(formatCurrencyValue(undefined)), 'R$ 0,00');
});

test('currency formatter renders negative values in BRL format', () => {
  assert.equal(normalizeCurrencyOutput(formatCurrencyValue(-42.5)), '-R$ 42,50');
});

test('currency formatter falls back to zero for invalid numeric input', () => {
  assert.equal(normalizeCurrencyOutput(formatCurrencyValue('abc')), 'R$ 0,00');
});
