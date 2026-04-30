import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

test('AppNav no longer exposes customer-facing operational freshness copy', () => {
  const source = readFileSync(new URL('../components/AppNav.tsx', import.meta.url), 'utf8');
  assert.ok(!source.includes('Frescor operacional'));
  assert.ok(!source.includes('describeLastSync('));
  assert.ok(!source.includes('describeSyncMessage('));
});

test('dashboard does not pin customer UX to sync bootstrap state', () => {
  const source = readFileSync(new URL('../dashboard/page.tsx', import.meta.url), 'utf8');
  assert.ok(!source.includes('initialSyncStatus='));
  assert.ok(!source.includes('publishedCoverageDate'));
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

test('pricing page keeps typed competitor prices while refetching after save', () => {
  const source = readFileSync(new URL('../pricing/page.tsx', import.meta.url), 'utf8');
  assert.ok(source.includes('router.replace(buildProductHref'));
  assert.ok(!source.includes('setPriceInputs({});'));
});

test('product navigation uses Plataforma label in Portuguese', () => {
  const source = readFileSync(new URL('../components/AppNav.tsx', import.meta.url), 'utf8');
  const platformShell = readFileSync(new URL('../components/PlatformShell.tsx', import.meta.url), 'utf8');
  assert.ok(source.includes('Plataforma'));
  assert.ok(!source.includes('>Platform<'));
  assert.ok(platformShell.includes('TorqMind Plataforma'));
});
