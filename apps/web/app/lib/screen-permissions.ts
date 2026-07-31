/**
 * ACL de menus e painéis (abas) — espelho do SCREEN_REGISTRY da API.
 * Fonte canônica: GET /platform/screen-registry e apps/api/app/permissions.py
 */

export type ScreenPanel = {
  key: string;
  label: string;
  requires_sensitive_role?: boolean;
};

export type ScreenMenu = {
  key: string;
  label: string;
  category?: string;
  kiosk_only?: boolean;
  panels: ScreenPanel[];
};

/** Fallback local se o registry da API falhar (mantém UI utilizável). */
export const FALLBACK_SCREEN_TREE: ScreenMenu[] = [
  {
    key: "sales",
    label: "Vendas",
    category: "Comercial",
    panels: [{ key: "sales.abc", label: "Curva ABC" }],
  },
  {
    key: "customers",
    label: "Clientes",
    category: "Comercial",
    panels: [],
  },
  {
    key: "inventory",
    label: "Estoque",
    category: "Comercial",
    panels: [],
  },
  {
    key: "competitor_pricing",
    label: "Preço Concorrente",
    category: "Comercial",
    panels: [
      { key: "competitor_pricing.register", label: "Registrar preços" },
      { key: "competitor_pricing.history", label: "Histórico" },
      { key: "competitor_pricing.comparison", label: "Comparativo" },
    ],
  },
  {
    key: "goals_team",
    label: "Metas",
    category: "Comercial",
    panels: [
      { key: "goals_team.metas", label: "Metas" },
      { key: "goals_team.comissoes", label: "Comissões" },
      { key: "goals_team.config", label: "Config. comissões" },
    ],
  },
  {
    key: "team",
    label: "Equipe",
    category: "Comercial",
    panels: [{ key: "team.custos", label: "Custo do funcionário" }],
  },
  {
    key: "cash",
    label: "Caixa",
    category: "Operação",
    panels: [],
  },
  {
    key: "fraud",
    label: "Antifraude",
    category: "Operação",
    panels: [
      { key: "fraud.core", label: "Cancelamentos e operadores" },
      { key: "fraud.risco_financeiro", label: "Risco financeiro / créditos" },
      { key: "fraud.credito_funcionario", label: "Crédito funcionário" },
    ],
  },
  {
    key: "fuel_loss",
    label: "Aferição de Combustível",
    category: "Operação",
    panels: [],
  },
  {
    key: "finance",
    label: "Financeiro",
    category: "Financeiro",
    panels: [
      { key: "finance.overview", label: "Geral (Pagar × Receber)" },
      { key: "finance.payable", label: "Contas a pagar" },
      { key: "finance.receivable", label: "Contas a receber" },
      { key: "finance.cheques", label: "Controle de cheques" },
      { key: "finance.despesas", label: "Despesas" },
      { key: "finance.budget", label: "Gestão orçamentária" },
    ],
  },
  {
    key: "profit_management",
    label: "Gestão de Lucro",
    category: "Financeiro",
    panels: [
      { key: "profit_management.overview", label: "Visão Geral (DRE)" },
      { key: "profit_management.products", label: "Produtos" },
      { key: "profit_management.repricing", label: "Oportunidades" },
      { key: "profit_management.solvencia", label: "Solvência", requires_sensitive_role: true },
      { key: "profit_management.anp", label: "Compliance ANP", requires_sensitive_role: true },
    ],
  },
];

export const TV_SCREEN_OPTIONS = [
  { key: "tv_sales_hourly", label: "TV — Vendas/Hora" },
  { key: "tv_sales_ranking", label: "TV — Ranking" },
];

export function allProductPermissionKeys(tree: ScreenMenu[]): string[] {
  const keys: string[] = [];
  for (const menu of tree) {
    if (menu.kiosk_only) continue;
    keys.push(menu.key);
    for (const panel of menu.panels) keys.push(panel.key);
  }
  return keys;
}

export function canAccessScreen(
  allowed: string[] | null | undefined,
  screenKey: string,
): boolean {
  if (!Array.isArray(allowed)) return true;
  return allowed.includes(screenKey);
}

export function toggleMenuPermission(
  current: string[],
  menu: ScreenMenu,
  checked: boolean,
): string[] {
  const panelKeys = menu.panels.map((p) => p.key);
  const without = current.filter((k) => k !== menu.key && !panelKeys.includes(k));
  if (!checked) return without;
  return [...without, menu.key, ...panelKeys];
}

export function togglePanelPermission(
  current: string[],
  menu: ScreenMenu,
  panelKey: string,
  checked: boolean,
): string[] {
  const panelKeys = menu.panels.map((p) => p.key);
  let next = current.filter((k) => k !== panelKey);
  if (checked) {
    if (!next.includes(menu.key)) next = [...next, menu.key];
    next = [...next, panelKey];
    return next;
  }
  const stillHasPanel = panelKeys.some((k) => k !== panelKey && next.includes(k));
  if (!stillHasPanel) {
    next = next.filter((k) => k !== menu.key);
  }
  return next;
}
