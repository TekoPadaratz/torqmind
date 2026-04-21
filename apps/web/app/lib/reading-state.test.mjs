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
    headline: 'Atualizando a leitura do dashboard geral',
    detail: 'Estamos fechando o novo recorte antes de liberar números e recomendações finais.',
  });

  assert.deepEqual(buildModuleUnavailableCopy('financeiro'), {
    headline: 'Ainda estamos fechando a leitura de financeiro',
    detail: 'Este recorte continua em atualização. Mantivemos a tela protegida para não exibir zero provisório ou dados misturados.',
  });
});
