import { buildScopeSearchParams } from './product-scope.mjs';

function normalizeBranchIds(scope = {}) {
  if (Array.isArray(scope?.id_filiais) && scope.id_filiais.length) {
    return scope.id_filiais.map((value) => String(value));
  }
  if (scope?.id_filial != null && String(scope.id_filial).trim() !== '') {
    return [String(scope.id_filial)];
  }
  return [];
}

export function resolvePricingOverviewRequest(scope = {}, session = null) {
  const branchIds = normalizeBranchIds(scope);
  const hasMultipleBranches = branchIds.length > 1;
  const branchId = !hasMultipleBranches
    ? (branchIds[0] || (session?.id_filial != null ? String(session.id_filial) : null))
    : null;
  const companyId = scope?.id_empresa != null && String(scope.id_empresa).trim() !== ''
    ? String(scope.id_empresa)
    : (session?.id_empresa != null ? String(session.id_empresa) : null);

  if (hasMultipleBranches) {
    return {
      branchId,
      companyId,
      error: 'Selecione apenas uma filial para usar o painel de preço da concorrência.',
      requestUrl: null,
    };
  }

  if (!branchId) {
    return {
      branchId: null,
      companyId,
      error: 'Selecione uma filial no escopo para usar o painel de preço da concorrência.',
      requestUrl: null,
    };
  }

  const params = buildScopeSearchParams({
    ...scope,
    id_filial: branchId,
    id_filiais: [branchId],
  });
  params.set('id_filial', branchId);
  params.delete('id_filiais');
  params.set('days_simulation', '10');
  if (companyId) params.set('id_empresa', companyId);

  return {
    branchId,
    companyId,
    error: null,
    requestUrl: `/bi/pricing/competitor/overview?${params.toString()}`,
  };
}
