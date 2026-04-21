import assert from 'node:assert/strict';
import test from 'node:test';

import { formatDurationHours } from './duration-format.mjs';

test('formatDurationHours renders sub-hour durations in minutes', () => {
  assert.equal(formatDurationHours(38 / 60), '38min');
});

test('formatDurationHours renders hours with minutes', () => {
  assert.equal(formatDurationHours(7.7), '7h 42min');
});

test('formatDurationHours renders multi-day durations without decimals', () => {
  assert.equal(formatDurationHours(27), '1d 3h');
});
