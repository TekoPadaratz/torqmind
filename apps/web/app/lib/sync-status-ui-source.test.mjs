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

test('dashboard data hook retries transient unavailable payloads without an infinite loop', () => {
  const source = readFileSync(new URL('./use-bi-scope-data.ts', import.meta.url), 'utf8');
  const dashboardSource = readFileSync(new URL('../dashboard/page.tsx', import.meta.url), 'utf8');
  const transitionSource = readFileSync(new URL('../components/ui/ScopeTransitionState.tsx', import.meta.url), 'utf8');
  assert.ok(source.includes('unavailableRetryAttempts = 4'));
  assert.ok(source.includes('unavailableRetryDelayMs = 2_000'));
  assert.ok(source.includes('attempt <= unavailableRetryAttempts'));
  assert.ok(source.includes('await waitBeforeRetry(unavailableRetryDelayMs)'));
  assert.ok(dashboardSource.includes('onRetry={pendingUnavailable ? () => window.location.reload() : undefined}'));
  assert.ok(transitionSource.includes('Tentar novamente'));
});

test('sales page uses customer-friendly sales labels', () => {
  const source = readFileSync(new URL('../sales/page.tsx', import.meta.url), 'utf8');
  assert.ok(source.includes('Vendas normais'));
  assert.ok(source.includes('Entradas registradas'));
  assert.ok(!source.includes('Saídas ativas'));
  assert.ok(!source.includes('Saídas normais'));
});
