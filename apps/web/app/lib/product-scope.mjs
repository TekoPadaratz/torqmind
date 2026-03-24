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

export function readScopeFromSearch(searchParams, fallback = {}) {
  const params =
    searchParams instanceof URLSearchParams
      ? searchParams
      : new URLSearchParams(typeof searchParams === 'string' ? searchParams : '');

  const dt_ini = params.get('dt_ini') || fallback.dt_ini || '';
  const dt_fim = params.get('dt_fim') || fallback.dt_fim || '';
  const dt_ref = params.get('dt_ref') || fallback.dt_ref || '';
  const id_empresa = params.get('id_empresa') || (fallback.id_empresa != null ? String(fallback.id_empresa) : null);
  const id_filial = params.get('id_filial') || (fallback.id_filial != null ? String(fallback.id_filial) : null);

  return {
    dt_ini,
    dt_fim,
    dt_ref,
    id_empresa,
    id_filial,
  };
}

export function buildScopeSearchParams(scope, options = {}) {
  const includeDtRef = Boolean(options.includeDtRef);
  const params = new URLSearchParams();
  if (scope?.dt_ini) params.set('dt_ini', String(scope.dt_ini));
  if (scope?.dt_fim) params.set('dt_fim', String(scope.dt_fim));
  if (scope?.id_empresa != null && String(scope.id_empresa).trim() !== '') params.set('id_empresa', String(scope.id_empresa));
  if (scope?.id_filial != null && String(scope.id_filial).trim() !== '') params.set('id_filial', String(scope.id_filial));
  if (includeDtRef && scope?.dt_ref) params.set('dt_ref', String(scope.dt_ref));
  return params;
}

export function buildProductHref(path, scope, options = {}) {
  const qs = buildScopeSearchParams(scope, options).toString();
  return qs ? `${path}?${qs}` : path;
}

export function getScopeControls(claims) {
  const userRole = String(claims?.user_role || claims?.role || '').toLowerCase();
  return {
    canSwitchCompany: userRole === 'platform_master' || userRole === 'product_global',
    canSwitchBranch: userRole === 'platform_master' || userRole === 'product_global' || userRole === 'tenant_admin',
    branchLocked: userRole === 'tenant_manager' && claims?.id_filial != null,
  };
}
