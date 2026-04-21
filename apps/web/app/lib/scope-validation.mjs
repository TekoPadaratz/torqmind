import { resolveAppliedBranchIds, uniqueBranchIds } from './branch-state.mjs';
import { normalizeCalendarDate } from './calendar-date.mjs';

export function resolveEffectiveBranchIds({
  branches = [],
  branchLocked = false,
  sessionBranchId = null,
  selectionMode = 'all',
  selectedBranchIds = [],
} = {}) {
  if (branchLocked) return uniqueBranchIds([sessionBranchId]);

  const availableBranchIds = uniqueBranchIds((branches || []).map((branch) => branch?.id_filial));
  if (selectionMode === 'all') return availableBranchIds;

  const appliedBranchIds = resolveAppliedBranchIds({
    branchLocked,
    sessionBranchId,
    selectionMode,
    selectedBranchIds,
  });

  if (!availableBranchIds.length) return appliedBranchIds;
  const allowed = new Set(availableBranchIds);
  return appliedBranchIds.filter((branchId) => allowed.has(branchId));
}

export function validateScopeDraft({
  branches = [],
  branchLocked = false,
  sessionBranchId = null,
  selectionMode = 'all',
  selectedBranchIds = [],
  dt_ini = '',
  dt_fim = '',
} = {}) {
  const normalizedStart = normalizeCalendarDate(dt_ini);
  if (!normalizedStart) {
    return {
      ok: false,
      error: 'Informe uma data inicial válida antes de aplicar o escopo.',
      effectiveBranchIds: [],
    };
  }

  const normalizedEnd = normalizeCalendarDate(dt_fim);
  if (!normalizedEnd) {
    return {
      ok: false,
      error: 'Informe uma data final válida antes de aplicar o escopo.',
      effectiveBranchIds: [],
    };
  }

  if (normalizedEnd < normalizedStart) {
    return {
      ok: false,
      error: 'A data final não pode ser menor que a data inicial.',
      effectiveBranchIds: [],
    };
  }

  const effectiveBranchIds = resolveEffectiveBranchIds({
    branches,
    branchLocked,
    sessionBranchId,
    selectionMode,
    selectedBranchIds,
  });

  if (!effectiveBranchIds.length) {
    return {
      ok: false,
      error: 'Selecione ao menos uma filial válida antes de aplicar o escopo.',
      effectiveBranchIds,
    };
  }

  return {
    ok: true,
    error: '',
    effectiveBranchIds,
  };
}

export function buildValidatedScope({
  draft = {},
  activeScope = {},
  effectiveBranchIds = [],
  scopeEpoch = '',
} = {}) {
  const branchIds = uniqueBranchIds(effectiveBranchIds);

  return {
    dt_ini: String(draft?.dt_ini || ''),
    dt_fim: String(draft?.dt_fim || ''),
    dt_ref: String(draft?.dt_fim || activeScope?.dt_ref || ''),
    scope_epoch: String(scopeEpoch || ''),
    id_empresa: String(draft?.id_empresa || activeScope?.id_empresa || ''),
    id_filiais: branchIds,
    id_filial: branchIds.length === 1 ? branchIds[0] : null,
  };
}
