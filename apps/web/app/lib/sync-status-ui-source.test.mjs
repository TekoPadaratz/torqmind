import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

test('AppNav no longer exposes customer-facing operational freshness copy', () => {
  const source = readFileSync(new URL('../components/AppNav.tsx', import.meta.url), 'utf8');
  assert.ok(!source.includes('Frescor operacional'));
  assert.ok(!source.includes('describeLastSync('));
  assert.ok(!source.includes('describeSyncMessage('));
});

test('legacy dashboard route redirects to sales', () => {
  const source = readFileSync(new URL('../dashboard/page.tsx', import.meta.url), 'utf8');
  assert.ok(source.includes('router.replace'));
  assert.ok(source.includes('/sales'));
  assert.ok(!source.includes('initialSyncStatus='));
});

test('bi scope data hook retries transient unavailable payloads without an infinite loop', () => {
  const source = readFileSync(new URL('./use-bi-scope-data.ts', import.meta.url), 'utf8');
  const salesSource = readFileSync(new URL('../sales/page.tsx', import.meta.url), 'utf8');
  const transitionSource = readFileSync(new URL('../components/ui/ScopeTransitionState.tsx', import.meta.url), 'utf8');
  assert.ok(source.includes('unavailableRetryAttempts = 4'));
  assert.ok(source.includes('unavailableRetryDelayMs = 2_000'));
  assert.ok(source.includes('attempt <= unavailableRetryAttempts'));
  assert.ok(source.includes('await waitBeforeRetry(unavailableRetryDelayMs)'));
  assert.ok(salesSource.includes('onRetry={pendingUnavailable ? () => window.location.reload() : undefined}') || transitionSource.includes('Tentar novamente'));
  assert.ok(transitionSource.includes('Tentar novamente'));
});

test('sales page uses customer-friendly sales labels', () => {
  const source = readFileSync(new URL('../sales/page.tsx', import.meta.url), 'utf8');
  assert.ok(source.includes('Vendas'));
  assert.ok(source.includes('Vendas por hora'));
  assert.ok(!source.includes('Saídas ativas'));
  assert.ok(!source.includes('Saídas normais'));
});

test('pricing page clears typed competitor prices after save to prevent stale data', () => {
  const source = readFileSync(new URL('../pricing/page.tsx', import.meta.url), 'utf8');
  assert.ok(source.includes('router.replace(buildProductHref'));
  assert.ok(source.includes('setPrices({})'));
});

test('product navigation uses Plataforma label in Portuguese', () => {
  const source = readFileSync(new URL('../components/AppNav.tsx', import.meta.url), 'utf8');
  const platformShell = readFileSync(new URL('../components/PlatformShell.tsx', import.meta.url), 'utf8');
  assert.ok(source.includes('Plataforma'));
  assert.ok(!source.includes('>Platform<'));
  assert.ok(platformShell.includes('TorqMind Plataforma'));
});

test('AppNav mobile scroll does not auto-reveal on scroll up', () => {
  const source = readFileSync(new URL('../components/AppNav.tsx', import.meta.url), 'utf8');
  // Must NOT contain "y < lastY" pattern (old auto-reveal on any scroll up)
  assert.ok(!source.includes('y < lastY'), 'scroll handler must not show nav on generic scroll up');
  // Must show nav only when near top (scrollY <= small threshold)
  assert.ok(source.includes('y <= 12') || source.includes('scrollY <= 12'), 'scroll handler must show nav only near top');
});
