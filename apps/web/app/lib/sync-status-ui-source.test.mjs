import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

test('AppNav still consults sync status unless the initial status is available', () => {
  const source = readFileSync(new URL('../components/AppNav.tsx', import.meta.url), 'utf8');
  assert.ok(source.includes('initialSyncStatus?.available === true'));
  assert.ok(!source.includes('if (initialSyncStatus) return;'));
});

test('dashboard does not freeze initial sync as unavailable when coverage exists', () => {
  const source = readFileSync(new URL('../dashboard/page.tsx', import.meta.url), 'utf8');
  assert.ok(source.includes('publishedCoverageDate'));
  assert.ok(source.includes('return operationalSync'));
  assert.ok(!source.includes('message: "A primeira base pronta ainda está sendo preparada."'));
});
