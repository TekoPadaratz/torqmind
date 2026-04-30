export const PRODUCT_LINKS = [
  { path: '/dashboard', label: 'Dashboard Geral' },
  { path: '/sales', label: 'Vendas' },
  { path: '/cash', label: 'Caixa' },
  { path: '/fraud', label: 'Antifraude' },
  { path: '/customers', label: 'Clientes' },
  { path: '/finance', label: 'Financeiro' },
  { path: '/pricing', label: 'Preço Concorrente' },
  { path: '/goals', label: 'Metas & Equipe' },
];

function normalizeScopeEpoch(rawValue) {
  const normalized = String(rawValue || '').trim();
  return normalized || null;
}

function normalizeBranchIds(...sources) {
  const values = [];

  for (const source of sources) {
    if (source == null || source === '') continue;
    if (Array.isArray(source)) {
      values.push(...source);
      continue;
    }
    if (typeof source === 'string' && source.includes(',')) {
      values.push(...source.split(','));
      continue;
    }
    values.push(source);
  }

  return [...new Set(values
    .map((value) => String(value).trim())
    .filter((value) => /^\d+$/.test(value) && Number(value) > 0))]
    .sort((left, right) => Number(left) - Number(right));
}

export function createScopeEpoch() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export function buildScopeKey(scope = {}) {
  return JSON.stringify({
    branch_ids: normalizeBranchIds(scope?.id_filiais, scope?.id_filial),
    dt_fim: scope?.dt_fim || '',
    dt_ini: scope?.dt_ini || '',
    dt_ref: scope?.dt_ref || '',
    id_empresa: scope?.id_empresa != null && String(scope.id_empresa).trim() !== '' ? String(scope.id_empresa) : null,
  });
}

export function readScopeFromSearch(searchParams, fallback = {}) {
  const params =
    searchParams instanceof URLSearchParams
      ? searchParams
      : new URLSearchParams(typeof searchParams === 'string' ? searchParams : '');

  const dt_ini = params.get('dt_ini') || fallback.dt_ini || '';
  const dt_fim = params.get('dt_fim') || fallback.dt_fim || '';
  const dt_ref = params.get('dt_ref') || fallback.dt_ref || '';
  const id_empresa = params.get('id_empresa') || (fallback.id_empresa != null ? String(fallback.id_empresa) : null);
  const requestedScopeEpoch = normalizeScopeEpoch(params.get('scope_epoch'));
  const fallbackScopeEpoch = normalizeScopeEpoch(fallback.scope_epoch);

  const requestedBranchScope = String(params.get('branch_scope') || '').trim().toLowerCase();
  const fallbackBranchScope = String(fallback.branch_scope || '').trim().toLowerCase();
  const branch_scope = requestedBranchScope || fallbackBranchScope || '';
  const requestedBranchIds = normalizeBranchIds(
    params.getAll('id_filiais'),
    params.get('id_filial'),
  );
  const fallbackBranchIds = normalizeBranchIds(
    fallback.id_filiais,
    fallback.id_filial,
  );
  const id_filiais = branch_scope === 'all'
    ? []
    : (requestedBranchIds.length ? requestedBranchIds : fallbackBranchIds);
  const id_filial = params.get('id_filial')
    || (id_filiais.length === 1 ? id_filiais[0] : null)
    || (fallback.id_filial != null ? String(fallback.id_filial) : null);
  const scope_key = buildScopeKey({ dt_ini, dt_fim, dt_ref, id_empresa, id_filial, id_filiais });

  return {
    dt_ini,
    dt_fim,
    dt_ref,
    id_empresa,
    id_filial,
    id_filiais,
    branch_scope,
    scope_epoch: requestedScopeEpoch || fallbackScopeEpoch || `legacy:${scope_key}`,
    scope_key,
  };
}

export function buildScopeSearchParams(scope, options = {}) {
  const includeDtRef = options.includeDtRef !== false;
  const params = new URLSearchParams();

  if (scope?.dt_ini) params.set('dt_ini', String(scope.dt_ini));
  if (scope?.dt_fim) params.set('dt_fim', String(scope.dt_fim));
  if (scope?.id_empresa != null && String(scope.id_empresa).trim() !== '') params.set('id_empresa', String(scope.id_empresa));

  const branchIds = normalizeBranchIds(scope?.id_filiais, scope?.id_filial);
  const branchScope = String(scope?.branch_scope || '').trim().toLowerCase();
  if (branchScope === 'all') {
    params.set('branch_scope', 'all');
  } else if (branchIds.length === 1) {
    params.set('id_filial', branchIds[0]);
  } else {
    for (const branchId of branchIds) params.append('id_filiais', branchId);
  }

  if (includeDtRef && scope?.dt_ref) params.set('dt_ref', String(scope.dt_ref));
  if (scope?.scope_epoch) params.set('scope_epoch', String(scope.scope_epoch));
  return params;
}

export function buildProductHref(path, scope, options = {}) {
  const qs = buildScopeSearchParams(scope, options).toString();
  return qs ? `${path}?${qs}` : path;
}

export function getScopeControls(claims) {
  const userRole = String(claims?.user_role || claims?.role || '').toLowerCase();
  const branchLocked = userRole === 'tenant_manager' && claims?.id_filial != null;
  const canSwitchCompany = userRole === 'platform_master' || userRole === 'product_global' || userRole === 'channel_admin';
  const canSwitchBranch = canSwitchCompany || userRole === 'tenant_admin';

  return {
    canSwitchCompany,
    canSwitchBranch,
    canSelectMultipleBranches: canSwitchBranch && !branchLocked,
    branchLocked,
  };
}
