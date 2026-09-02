import { buildBrowserLocalDefaultScope } from './local-scope-defaults.mjs';

/** Leaf product routes (used for scope prefetch + ACL flat filter). */
export const PRODUCT_LINKS = [
  { path: '/sales', label: 'Vendas', screen_key: 'sales' },
  { path: '/sales/abc', label: 'Curva ABC', screen_key: 'sales.abc', parent_screen: 'sales' },
  { path: '/customers', label: 'Clientes', screen_key: 'customers' },
  { path: '/team', label: 'Equipe', screen_key: 'team' },
  { path: '/inventory', label: 'Estoque de combustível', screen_key: 'inventory' },
  { path: '/goals', label: 'Metas', screen_key: 'goals_team' },
  { path: '/goals?tab=comissoes', label: 'Comissões', screen_key: 'goals_team.comissoes', parent_screen: 'goals_team' },
  { path: '/pricing', label: 'Preço Concorrente', screen_key: 'competitor_pricing' },
  { path: '/cash', label: 'Caixa', screen_key: 'cash' },
  { path: '/fraud', label: 'Antifraude', screen_key: 'fraud' },
  { path: '/fuel-loss', label: 'Movimentações de Combustível', screen_key: 'fuel_loss' },
  { path: '/product-management', label: 'Gestão de Produtos', screen_key: 'product_management' },
  { path: '/finance?view=receivable', label: 'Contas a receber', screen_key: 'finance.receivable', parent_screen: 'finance' },
  { path: '/finance?view=cheques', label: 'Controle de cheques', screen_key: 'finance.cheques', parent_screen: 'finance' },
  { path: '/finance?view=despesas', label: 'Despesas', screen_key: 'finance.despesas', parent_screen: 'finance' },
  { path: '/finance', label: 'Geral (Pagar × Receber)', screen_key: 'finance.overview', parent_screen: 'finance' },
  { path: '/profit-management', label: 'Gestão de Lucro', screen_key: 'profit_management' },
  { path: '/finance?view=budget', label: 'Gestão orçamentária', screen_key: 'finance.budget', parent_screen: 'finance' },
];

/**
 * Top-nav domains with flyout children — ordem alfabética por label dentro do grupo.
 */
export const PRODUCT_NAV_GROUPS = [
  {
    id: 'comercial',
    label: 'Comercial',
    children: [
      { path: '/customers', label: 'Clientes', screen_key: 'customers' },
      { path: '/goals?tab=comissoes', label: 'Comissões', screen_key: 'goals_team.comissoes', parent_screen: 'goals_team' },
      { path: '/sales/abc', label: 'Curva ABC', screen_key: 'sales.abc', parent_screen: 'sales' },
      { path: '/team', label: 'Equipe', screen_key: 'team' },
      { path: '/inventory', label: 'Estoque de combustível', screen_key: 'inventory' },
      { path: '/goals', label: 'Metas', screen_key: 'goals_team' },
      { path: '/pricing', label: 'Preço Concorrente', screen_key: 'competitor_pricing' },
      { path: '/sales', label: 'Vendas', screen_key: 'sales' },
    ],
  },
  {
    id: 'financeiro',
    label: 'Financeiro',
    children: [
      { path: '/finance?view=payable', label: 'Contas a pagar', screen_key: 'finance.payable', parent_screen: 'finance' },
      { path: '/finance?view=receivable', label: 'Contas a receber', screen_key: 'finance.receivable', parent_screen: 'finance' },
      { path: '/finance?view=cheques', label: 'Controle de cheques', screen_key: 'finance.cheques', parent_screen: 'finance' },
      { path: '/finance?view=despesas', label: 'Despesas', screen_key: 'finance.despesas', parent_screen: 'finance' },
      { path: '/finance', label: 'Geral (Pagar × Receber)', screen_key: 'finance.overview', parent_screen: 'finance' },
      { path: '/profit-management', label: 'Gestão de Lucro', screen_key: 'profit_management' },
      { path: '/finance?view=budget', label: 'Gestão orçamentária', screen_key: 'finance.budget', parent_screen: 'finance' },
    ],
  },
  {
    id: 'operacao',
    label: 'Operação',
    children: [
      { path: '/fraud', label: 'Antifraude', screen_key: 'fraud' },
      { path: '/cash', label: 'Caixa', screen_key: 'cash' },
      { path: '/product-management', label: 'Gestão de Produtos', screen_key: 'product_management' },
      { path: '/fuel-loss', label: 'Movimentações de Combustível', screen_key: 'fuel_loss' },
    ],
  },
];

/**
 * Espelho de SCREEN_REGISTRY (menu → painéis). Nav só exibe chaves presentes no ACL.
 */
const REGISTRY_PANELS_BY_MENU = {
  sales: ['sales.overview', 'sales.evolution', 'sales.hourly', 'sales.top', 'sales.abc'],
  fraud: ['fraud.core', 'fraud.risco_financeiro', 'fraud.credito_funcionario'],
  finance: [
    'finance.overview',
    'finance.payable',
    'finance.receivable',
    'finance.cheques',
    'finance.despesas',
    'finance.budget',
  ],
  goals_team: ['goals_team.metas', 'goals_team.comissoes', 'goals_team.gerente', 'goals_team.config'],
  competitor_pricing: [
    'competitor_pricing.register',
    'competitor_pricing.history',
    'competitor_pricing.comparison',
  ],
  profit_management: [
    'profit_management.overview',
    'profit_management.products',
    'profit_management.repricing',
    'profit_management.solvencia',
    'profit_management.anp',
  ],
  team: ['team.custos'],
};

function menuHasRegisteredPanels(menuKey) {
  const panels = REGISTRY_PANELS_BY_MENU[menuKey];
  return Array.isArray(panels) && panels.length > 0;
}

function anyRegisteredPanelGranted(set, menuKey) {
  const panels = REGISTRY_PANELS_BY_MENU[menuKey];
  if (!panels?.length) return false;
  return panels.some((key) => set.has(key));
}

/** Alinha com expand_screen_permissions da API (legado: menu sem filhos → todos os painéis). */
function expandAllowedScreens(allowed_screens) {
  if (!Array.isArray(allowed_screens)) return allowed_screens;
  const result = new Set(allowed_screens);
  for (const [menu, panels] of Object.entries(REGISTRY_PANELS_BY_MENU)) {
    const hasParent = result.has(menu);
    const hasChild = panels.some((key) => result.has(key));
    if (hasParent && !hasChild) {
      for (const key of panels) result.add(key);
    } else if (hasChild && !hasParent) {
      result.add(menu);
    }
  }
  return [...result];
}

function screenAllowed(set, link) {
  if (set.has(link.screen_key)) return true;

  const key = link.screen_key;
  const parent = link.parent_screen;

  // Rota de entrada (/sales, /goals) quando algum painel filho está permitido.
  if (!parent && menuHasRegisteredPanels(key) && anyRegisteredPanelGranted(set, key)) {
    return true;
  }

  // Painel ou item de nav filho: só com chave explícita — pai não abre siblings desmarcados.
  if (parent || key.includes('.')) {
    return false;
  }

  // Menu sem painéis (cash, customers, …): chave do menu basta.
  return set.has(key);
}

/**
 * Filter PRODUCT_LINKS to only those the user has screen access to.
 * If allowed_screens is null/undefined (admin users), show all.
 * If allowed_screens is an empty array, show none (user has no permissions).
 */
export function filterProductLinks(allowed_screens) {
  if (!Array.isArray(allowed_screens)) {
    return PRODUCT_LINKS;
  }
  if (allowed_screens.length === 0) {
    return [];
  }
  const set = new Set(expandAllowedScreens(allowed_screens));
  return PRODUCT_LINKS.filter((link) => screenAllowed(set, link));
}

/** Filter nav groups for flyout rendering. */
export function filterProductNavGroups(allowed_screens) {
  const sortChildren = (groups) =>
    groups.map((group) => ({
      ...group,
      children: [...group.children].sort((a, b) => a.label.localeCompare(b.label, 'pt-BR')),
    }));

  if (!Array.isArray(allowed_screens)) {
    return sortChildren(PRODUCT_NAV_GROUPS);
  }
  if (allowed_screens.length === 0) {
    return [];
  }
  const set = new Set(expandAllowedScreens(allowed_screens));
  return PRODUCT_NAV_GROUPS
    .map((group) => ({
      ...group,
      children: group.children
        .filter((link) => screenAllowed(set, link))
        .sort((a, b) => a.label.localeCompare(b.label, 'pt-BR')),
    }))
    .filter((group) => group.children.length > 0);
}

function normalizeScopeEpoch(rawValue) {
  const normalized = String(rawValue || '').trim();
  return normalized || null;
}

const SCOPE_QUERY_KEYS = [
  'dt_ini',
  'dt_fim',
  'dt_ref',
  'id_empresa',
  'id_filial',
  'id_filiais',
  'branch_scope',
  'scope_epoch',
];

function normalizeProductPath(rawPath) {
  const fallbackPath = typeof rawPath === 'string' && rawPath.trim() ? rawPath.trim() : '/sales';
  const normalizedPath = fallbackPath.startsWith('/') ? fallbackPath : `/${fallbackPath}`;
  const url = new URL(normalizedPath, 'https://torqmind.local');

  return {
    pathname: url.pathname || '/sales',
    searchParams: new URLSearchParams(url.searchParams),
  };
}

function normalizeBranchScope(rawScope) {
  return String(rawScope || '').trim().toLowerCase();
}

function hasExplicitScope(searchParams) {
  const hasDt = Boolean(searchParams.get('dt_ini')) && Boolean(searchParams.get('dt_fim'));
  const hasEmpresa = Boolean(searchParams.get('id_empresa'));
  const hasBranch = Boolean(searchParams.get('id_filial'))
    || searchParams.getAll('id_filiais').length > 0
    || String(searchParams.get('branch_scope') || '').trim().toLowerCase() === 'all';
  const hasScopeEpoch = Boolean(normalizeScopeEpoch(searchParams.get('scope_epoch')));

  return hasDt && hasEmpresa && hasBranch && hasScopeEpoch;
}

export function hasExplicitBranchSelection(searchParams) {
  const params =
    searchParams instanceof URLSearchParams
      ? searchParams
      : new URLSearchParams(typeof searchParams === 'string' ? searchParams : '');

  const branchScope = normalizeBranchScope(params.get('branch_scope'));
  return Boolean(params.get('id_filial'))
    || params.getAll('id_filiais').length > 0
    || branchScope === 'all'
    || branchScope === 'selected';
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

export function needsCanonicalScope(path) {
  const { searchParams } = normalizeProductPath(path);
  return !hasExplicitScope(searchParams);
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
  const explicitBranchSelection = hasExplicitBranchSelection(params);

  const dt_ini = params.get('dt_ini') || fallback.dt_ini || '';
  const dt_fim = params.get('dt_fim') || fallback.dt_fim || '';
  const dt_ref = params.get('dt_ref') || fallback.dt_ref || '';
  const id_empresa = params.get('id_empresa') || (fallback.id_empresa != null ? String(fallback.id_empresa) : null);
  const requestedScopeEpoch = normalizeScopeEpoch(params.get('scope_epoch'));
  const fallbackScopeEpoch = normalizeScopeEpoch(fallback.scope_epoch);

  const requestedBranchScope = normalizeBranchScope(params.get('branch_scope'));
  const fallbackBranchScope = normalizeBranchScope(fallback.branch_scope);
  const branch_scope = explicitBranchSelection ? requestedBranchScope : (requestedBranchScope || fallbackBranchScope || '');
  const requestedBranchIds = normalizeBranchIds(
    params.getAll('id_filiais'),
    params.get('id_filial'),
  );
  const fallbackBranchIds = normalizeBranchIds(
    fallback.id_filiais,
    fallback.id_filial,
  );
  const id_filiais = explicitBranchSelection
    ? requestedBranchIds
    : (requestedBranchIds.length ? requestedBranchIds : fallbackBranchIds);
  const id_filial = explicitBranchSelection
    ? (params.get('id_filial') || (requestedBranchIds.length === 1 ? requestedBranchIds[0] : null))
    : (params.get('id_filial')
      || (id_filiais.length === 1 ? id_filiais[0] : null)
      || (fallback.id_filial != null ? String(fallback.id_filial) : null));
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
  const { pathname, searchParams: pathParams } = normalizeProductPath(path);
  const merged = new URLSearchParams(pathParams);
  const scopeParams = buildScopeSearchParams(scope, options);
  for (const [key, value] of scopeParams.entries()) {
    if (SCOPE_QUERY_KEYS.includes(key)) {
      merged.delete(key);
    }
  }
  for (const [key, value] of scopeParams.entries()) {
    merged.append(key, value);
  }
  const qs = merged.toString();
  return qs ? `${pathname}?${qs}` : pathname;
}

export function buildCanonicalProductHref(path, session, options = {}) {
  const { pathname, searchParams } = normalizeProductPath(path);
  const fallbackScope = buildBrowserLocalDefaultScope(session);
  const explicitBranchSelection = hasExplicitBranchSelection(searchParams);
  const parsedScope = readScopeFromSearch(searchParams, fallbackScope);
  const requestedDtRef = searchParams.get('dt_ref');
  const scope = {
    ...parsedScope,
    dt_ini: parsedScope.dt_ini || fallbackScope.dt_ini || '',
    dt_fim: parsedScope.dt_fim || fallbackScope.dt_fim || '',
    dt_ref: requestedDtRef || parsedScope.dt_fim || fallbackScope.dt_ref || fallbackScope.dt_fim || '',
    id_empresa: parsedScope.id_empresa || fallbackScope.id_empresa || null,
    id_filial: explicitBranchSelection
      ? (parsedScope.id_filial || null)
      : (parsedScope.id_filial || fallbackScope.id_filial || null),
    id_filiais: explicitBranchSelection
      ? (parsedScope.id_filiais || [])
      : (parsedScope.id_filiais?.length ? parsedScope.id_filiais : (fallbackScope.id_filiais || [])),
    branch_scope: explicitBranchSelection
      ? (parsedScope.branch_scope || '')
      : (parsedScope.branch_scope || fallbackScope.branch_scope || ''),
    scope_epoch: normalizeScopeEpoch(options.scopeEpoch)
      || normalizeScopeEpoch(parsedScope.scope_epoch)
      || createScopeEpoch(),
  };

  const mergedParams = new URLSearchParams(searchParams);
  for (const key of SCOPE_QUERY_KEYS) {
    mergedParams.delete(key);
  }

  const scopeParams = buildScopeSearchParams(scope, options);
  for (const [key, value] of scopeParams.entries()) {
    mergedParams.append(key, value);
  }

  const query = mergedParams.toString();
  return query ? `${pathname}?${query}` : pathname;
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
