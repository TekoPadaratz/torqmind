import assert from 'node:assert/strict';
import test from 'node:test';

import {
  formatGoalTargetInputFromNumber,
  normalizeGoalTargetInput,
  parseGoalTargetInput,
} from './goal-target-input.mjs';

test('goal target input normalizes manual typing into BRL currency', () => {
  assert.equal(normalizeGoalTargetInput('1'), 'R$ 0,01');
  assert.equal(normalizeGoalTargetInput('123456'), 'R$ 1.234,56');
});

test('goal target input parses the displayed currency back into a numeric payload', () => {
  assert.equal(parseGoalTargetInput('R$ 5.700,25'), 5700.25);
});

test('goal target input formats stored goal values for reload', () => {
  assert.equal(formatGoalTargetInputFromNumber(5700.25), 'R$ 5.700,25');
});
