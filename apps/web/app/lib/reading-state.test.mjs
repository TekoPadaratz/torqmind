import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildModuleLoadingCopy,
  buildModuleUnavailableCopy,
  isScopePayloadStable,
} from './reading-state.mjs';

test('scope payload stability rejects protected, mixed or preparing payloads', () => {
  assert.equal(
    isScopePayloadStable({
      _snapshot_cache: { exact_scope_match: true, mode: 'live_computed' },
    }),
    true,
  );

  assert.equal(
    isScopePayloadStable({
      _snapshot_cache: { exact_scope_match: false, mode: 'protected_snapshot' },
    }),
    false,
  );

  assert.equal(
    isScopePayloadStable({
      _snapshot_cache: { exact_scope_match: true, mode: 'protected_unavailable' },
    }),
    false,
  );

  assert.equal(
    isScopePayloadStable({
      _snapshot_cache: { exact_scope_match: true, mode: 'refreshing' },
      _fallback_meta: { fallback_state: 'preparing' },
    }),
    false,
  );
});

test('scope loading copy is explicit about waiting for final numbers', () => {
  assert.deepEqual(buildModuleLoadingCopy('o dashboard geral'), {
    headline: 'Atualizando os dados do dashboard geral',
    detail: 'Estamos preparando os números deste período. Isso costuma levar poucos segundos.',
  });

  assert.deepEqual(buildModuleUnavailableCopy('financeiro'), {
    headline: 'Ainda estamos atualizando os dados de financeiro',
    detail: 'Os números deste período ainda não ficaram prontos. Tente novamente em instantes.',
  });
});
