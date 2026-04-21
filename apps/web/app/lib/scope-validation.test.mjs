import assert from 'node:assert/strict';
import test from 'node:test';

import { buildScopeSearchParams } from './product-scope.mjs';
import { buildValidatedScope, resolveEffectiveBranchIds, validateScopeDraft } from './scope-validation.mjs';

const branches = [
  { id_filial: 11, nome: 'Filial 11' },
  { id_filial: 13, nome: 'Filial 13' },
];

test('validateScopeDraft accepts a valid date range with resolved branches', () => {
  const result = validateScopeDraft({
    branches,
    selectionMode: 'selected',
    selectedBranchIds: ['11'],
    dt_ini: '2026-04-01',
    dt_fim: '2026-04-14',
  });

  assert.equal(result.ok, true);
  assert.deepEqual(result.effectiveBranchIds, ['11']);
});

test('validateScopeDraft rejects reversed dates', () => {
  const result = validateScopeDraft({
    branches,
    selectionMode: 'selected',
    selectedBranchIds: ['11'],
    dt_ini: '2026-04-14',
    dt_fim: '2026-04-01',
  });

  assert.equal(result.ok, false);
  assert.match(result.error, /data final/i);
});

test('validateScopeDraft rejects invalid calendar dates without UTC round-trips', () => {
  const result = validateScopeDraft({
    branches,
    selectionMode: 'selected',
    selectedBranchIds: ['11'],
    dt_ini: '2026-02-31',
    dt_fim: '2026-03-01',
  });

  assert.equal(result.ok, false);
  assert.match(result.error, /data inicial válida/i);
});

test('validateScopeDraft rejects empty effective branch scope', () => {
  const result = validateScopeDraft({
    branches: [],
    selectionMode: 'selected',
    selectedBranchIds: [],
    dt_ini: '2026-04-01',
    dt_fim: '2026-04-14',
  });

  assert.equal(result.ok, false);
  assert.match(result.error, /filial válida/i);
});

test('validateScopeDraft keeps branch-locked scope valid with the session branch', () => {
  const result = validateScopeDraft({
    branches: [],
    branchLocked: true,
    sessionBranchId: 77,
    selectionMode: 'all',
    selectedBranchIds: [],
    dt_ini: '2026-04-01',
    dt_fim: '2026-04-14',
  });

  assert.equal(result.ok, true);
  assert.deepEqual(result.effectiveBranchIds, ['77']);
});

test('resolveEffectiveBranchIds expands the all-branches selection into concrete branch ids', () => {
  assert.deepEqual(
    resolveEffectiveBranchIds({
      branches,
      selectionMode: 'all',
      selectedBranchIds: [],
    }),
    ['11', '13'],
  );
});

test('validateScopeDraft filters stale branch ids out of the effective scope', () => {
  const result = validateScopeDraft({
    branches,
    selectionMode: 'selected',
    selectedBranchIds: ['11', '999'],
    dt_ini: '2026-04-01',
    dt_fim: '2026-04-14',
  });

  assert.equal(result.ok, true);
  assert.deepEqual(result.effectiveBranchIds, ['11']);
});

test('buildValidatedScope persists only the validated effective branches', () => {
  const validation = validateScopeDraft({
    branches,
    selectionMode: 'selected',
    selectedBranchIds: ['11', '999'],
    dt_ini: '2026-04-01',
    dt_fim: '2026-04-14',
  });

  const scope = buildValidatedScope({
    draft: {
      dt_ini: '2026-04-01',
      dt_fim: '2026-04-14',
      id_empresa: '5',
      id_filiais: ['11', '999'],
    },
    activeScope: {
      dt_ref: '2026-04-14',
      id_empresa: '5',
    },
    effectiveBranchIds: validation.effectiveBranchIds,
    scopeEpoch: 'scope-1',
  });

  const params = buildScopeSearchParams(scope).toString();
  assert.deepEqual(scope.id_filiais, ['11']);
  assert.equal(scope.id_filial, '11');
  assert.equal(params.includes('999'), false);
  assert.match(params, /id_filial=11/);
});

test('buildValidatedScope keeps the locked branch after validation', () => {
  const validation = validateScopeDraft({
    branches: [],
    branchLocked: true,
    sessionBranchId: 77,
    selectionMode: 'selected',
    selectedBranchIds: ['11'],
    dt_ini: '2026-04-01',
    dt_fim: '2026-04-14',
  });

  const scope = buildValidatedScope({
    draft: {
      dt_ini: '2026-04-01',
      dt_fim: '2026-04-14',
      id_empresa: '9',
      id_filiais: ['11'],
    },
    activeScope: {
      dt_ref: '2026-04-14',
      id_empresa: '9',
    },
    effectiveBranchIds: validation.effectiveBranchIds,
    scopeEpoch: 'scope-2',
  });

  const params = buildScopeSearchParams(scope).toString();
  assert.deepEqual(scope.id_filiais, ['77']);
  assert.equal(scope.id_filial, '77');
  assert.equal(params.includes('11'), false);
  assert.match(params, /id_filial=77/);
});
