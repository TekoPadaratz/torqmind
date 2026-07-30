'use client';

import Image from 'next/image';
import Link from 'next/link';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { startTransition, useEffect, useMemo, useRef, useState } from 'react';

import { apiGet } from '../lib/api';
import { clearAuth } from '../lib/auth';
import { getVisibleBranches, uniqueBranchIds } from '../lib/branch-state.mjs';
import { buildQuickShortcutRanges, formatBusinessCalendarDate, parseCalendarDate } from '../lib/calendar-date.mjs';
import { buildBrowserLocalDefaultScope } from '../lib/local-scope-defaults.mjs';
import { describeServerBaseDate } from '../lib/reading-copy.mjs';
import { clearSessionCache, loadSession, readCachedSession } from '../lib/session';
import { buildValidatedScope, validateScopeDraft } from '../lib/scope-validation.mjs';
import { prefetchProductScope, startScopeTransition, useScopeTransitionState } from '../lib/scope-runtime';
import {
  buildProductHref,
  buildScopeSearchParams,
  createScopeEpoch,
  filterProductNavGroups,
  getScopeControls,
  hasExplicitBranchSelection,
  readScopeFromSearch,
} from '../lib/product-scope.mjs';
import ThemeToggleButton from './ThemeToggleButton';

const FILTER_DOCK_KEY = 'tm.filterDock';
const TOPNAV_KEY = 'tm.topNav';

function linkIsActive(pathname: string, searchParams: URLSearchParams, itemPath: string) {
  const url = new URL(itemPath, 'https://torqmind.local');
  if (pathname !== url.pathname) return false;
  let paramsMatch = true;
  url.searchParams.forEach((value, key) => {
    if (searchParams.get(key) !== value) paramsMatch = false;
  });
  if (!paramsMatch) return false;
  // Exact finance overview: no view= when item has none
  if (url.pathname === '/finance' && !url.searchParams.has('view')) {
    return !searchParams.get('view');
  }
  if (url.pathname === '/goals' && !url.searchParams.has('tab')) {
    const tab = searchParams.get('tab');
    return !tab || tab === 'metas';
  }
  // /sales should not stay active on /sales/abc
  if (url.pathname === '/sales' && !itemPath.includes('/abc')) {
    return pathname === '/sales';
  }
  return true;
}

function groupIsActive(pathname: string, children: { path: string }[]) {
  return children.some((child) => {
    const url = new URL(child.path, 'https://torqmind.local');
    return pathname === url.pathname;
  });
}

type BranchOption = {
  id_filial: number;
  nome: string;
};

type ScopeDraft = {
  dt_ini: string;
  dt_fim: string;
  id_empresa: string;
  id_filiais: string[];
  selectionMode: 'all' | 'selected';
};

function scopeFromSession(searchParams: URLSearchParams, session: any) {
  const fallback = buildBrowserLocalDefaultScope(session);
  const parsed = readScopeFromSearch(searchParams, fallback);
  const controls = getScopeControls(session);
  const explicitBranchSelection = hasExplicitBranchSelection(searchParams);

  const fallbackCompany =
    parsed.id_empresa ||
    (fallback.id_empresa != null ? String(fallback.id_empresa) : null) ||
    (session?.id_empresa != null ? String(session.id_empresa) : null) ||
    (session?.tenant_ids?.length ? String(session.tenant_ids[0]) : null);

  const explicitBranchIds = uniqueBranchIds([
    ...(parsed.id_filiais || []),
    parsed.id_filial,
  ]);

  const fallbackBranchIds = controls.branchLocked && session?.id_filial != null
    ? [String(session.id_filial)]
    : explicitBranchSelection
      ? explicitBranchIds
      : uniqueBranchIds([
          ...(parsed.id_filiais || []),
          ...(fallback.id_filiais || []),
          parsed.id_filial,
          fallback.id_filial,
          session?.id_filial,
        ]);

  const branchScope = explicitBranchSelection
    ? (parsed.branch_scope || (fallbackBranchIds.length ? 'selected' : 'all'))
    : (parsed.branch_scope || fallback.branch_scope || (fallbackBranchIds.length ? 'selected' : 'all'));

  return {
    dt_ini: parsed.dt_ini || fallback.dt_ini || '',
    dt_fim: parsed.dt_fim || fallback.dt_fim || '',
    dt_ref: parsed.dt_ref || fallback.dt_ref || '',
    scope_epoch: parsed.scope_epoch || fallback.scope_epoch || '',
    scope_key: parsed.scope_key || fallback.scope_key || '',
    id_empresa: fallbackCompany,
    id_filial: fallbackBranchIds.length === 1 ? fallbackBranchIds[0] : null,
    id_filiais: fallbackBranchIds,
    branch_scope: branchScope,
  };
}

function companyLabel(session: any, idEmpresa: string | null) {
  if (!idEmpresa) return 'Empresa não definida';
  const companies = session?.product_companies || [];
  const match = companies.find((item: any) => String(item.id_empresa) === String(idEmpresa));
  if (!match) return `Empresa ${idEmpresa}`;
  return `${match.tenant_name || `Empresa ${idEmpresa}`}`;
}

function branchSelectionLabel(branches: BranchOption[], selectedIds: string[], selectionMode: 'all' | 'selected', locked = false) {
  if (!locked && selectionMode === 'all') return 'Todas as filiais';
  if (!selectedIds.length) return locked ? 'Filial indisponível' : 'Todas as filiais';
  if (selectedIds.length === 1) {
    const branch = branches.find((item) => String(item.id_filial) === selectedIds[0]);
    return branch?.nome || `Filial ${selectedIds[0]}`;
  }
  return `${selectedIds.length} filiais selecionadas`;
}

export default function AppNav({
  title,
  userLabel,
  initialUnread,
  deferAuxiliaryLoads = false,
  hideScopeOnMobile = false,
}: {
  title: string;
  userLabel?: string;
  initialUnread?: number;
  deferAuxiliaryLoads?: boolean;
  hideScopeOnMobile?: boolean;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const [session, setSession] = useState<any>(null);
  const [draft, setDraft] = useState<ScopeDraft>({
    dt_ini: '',
    dt_fim: '',
    id_empresa: '',
    id_filiais: [],
    selectionMode: 'all',
  });
  const [branchSearch, setBranchSearch] = useState('');
  const [branches, setBranches] = useState<BranchOption[]>([]);
  const [loadingBranches, setLoadingBranches] = useState(false);
  const [unread, setUnread] = useState(initialUnread ?? 0);
  const [alertsOpen, setAlertsOpen] = useState(false);
  const [alertsLoading, setAlertsLoading] = useState(false);
  const [alerts, setAlerts] = useState<any[]>([]);
  const [auxiliaryLoadsEnabled, setAuxiliaryLoadsEnabled] = useState(!deferAuxiliaryLoads);
  const scopeTransition = useScopeTransitionState();
  const [navHidden, setNavHidden] = useState(false);
  const [topNavCollapsed, setTopNavCollapsed] = useState(false);
  const headerRef = useRef<HTMLElement | null>(null);
  const [filterDockCollapsed, setFilterDockCollapsed] = useState(false);
  const [openFlyout, setOpenFlyout] = useState<string | null>(null);
  const flyoutCloseTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    document.body.classList.add('product-shell');
    return () => document.body.classList.remove('product-shell');
  }, []);

  useEffect(() => {
    try {
      const stored = window.localStorage.getItem(FILTER_DOCK_KEY);
      if (stored === 'collapsed') setFilterDockCollapsed(true);
      const top = window.localStorage.getItem(TOPNAV_KEY);
      if (top === 'collapsed') setTopNavCollapsed(true);
    } catch {
      /* ignore */
    }
  }, []);

  useEffect(() => {
    document.body.classList.toggle('filter-dock-collapsed', filterDockCollapsed);
    document.body.style.setProperty('--sidebar-w', filterDockCollapsed ? '56px' : '316px');
    try {
      window.localStorage.setItem(FILTER_DOCK_KEY, filterDockCollapsed ? 'collapsed' : 'pinned');
    } catch {
      /* ignore */
    }
  }, [filterDockCollapsed]);

  useEffect(() => {
    document.body.classList.toggle('topnav-collapsed', topNavCollapsed);
    try {
      window.localStorage.setItem(TOPNAV_KEY, topNavCollapsed ? 'collapsed' : 'pinned');
    } catch {
      /* ignore */
    }
  }, [topNavCollapsed]);

  useEffect(() => {
    return () => {
      document.body.classList.remove('filter-dock-collapsed');
      document.body.classList.remove('topnav-collapsed');
      document.body.style.removeProperty('--sidebar-w');
    };
  }, []);

  // Segurança de scroll: ao clicar na barra (sidebar ou documento), limpa seleção
  // residual que faz o browser "selecionar texto" em vez de arrastar o thumb.
  useEffect(() => {
    const clearSelectionNearScrollbar = (event: MouseEvent) => {
      if (event.button !== 0) return;
      const x = event.clientX;
      const y = event.clientY;
      const nearDocScrollbar = x >= window.innerWidth - 18;
      let nearSidebarScrollbar = false;
      const sidebar = document.querySelector('.productSidebar') as HTMLElement | null;
      if (sidebar) {
        const rect = sidebar.getBoundingClientRect();
        nearSidebarScrollbar =
          x >= rect.right - 16
          && x <= rect.right + 4
          && y >= rect.top
          && y <= rect.bottom;
      }
      if (nearDocScrollbar || nearSidebarScrollbar) {
        const sel = window.getSelection();
        if (sel && sel.rangeCount > 0) sel.removeAllRanges();
      }
    };
    document.addEventListener('mousedown', clearSelectionNearScrollbar, true);
    return () => document.removeEventListener('mousedown', clearSelectionNearScrollbar, true);
  }, []);

  // Hide nav on scroll down (mobile), show only when near the top
  useEffect(() => {
    let ticking = false;
    const onScroll = () => {
      if (ticking) return;
      ticking = true;
      requestAnimationFrame(() => {
        const y = window.scrollY;
        if (y <= 12) {
          setNavHidden(false);
        } else if (y > 80) {
          setNavHidden(true);
        }
        ticking = false;
      });
    };
    window.addEventListener('scroll', onScroll, { passive: true });
    return () => window.removeEventListener('scroll', onScroll);
  }, []);

  useEffect(() => {
    let active = true;

    const hydrateSession = async () => {
      const cached = readCachedSession();
      if (active && cached) setSession(cached);
      const me = await loadSession(router, 'product');
      if (active && me) setSession(me);
    };

    hydrateSession();
    return () => {
      active = false;
    };
  }, [router]);

  useEffect(() => {
    if (typeof initialUnread === 'number') {
      setUnread(initialUnread);
    }
  }, [initialUnread]);

  // Keep the page content offset and sidebar position in sync with the REAL
  // height of the fixed top navigation. When the menu wraps to 3+ lines the
  // header grows; without this the content would be covered by the bar.
  useEffect(() => {
    const header = headerRef.current;
    if (!header || typeof window === 'undefined') return;
    const applyHeight = () => {
      document.body.style.setProperty('--product-nav-h', `${header.offsetHeight}px`);
    };
    applyHeight();
    let observer: ResizeObserver | null = null;
    if (typeof ResizeObserver !== 'undefined') {
      observer = new ResizeObserver(applyHeight);
      observer.observe(header);
    }
    window.addEventListener('resize', applyHeight);
    return () => {
      observer?.disconnect();
      window.removeEventListener('resize', applyHeight);
      document.body.style.removeProperty('--product-nav-h');
    };
  }, [session]);

  useEffect(() => {
    if (!deferAuxiliaryLoads) {
      setAuxiliaryLoadsEnabled(true);
      return;
    }
    if (auxiliaryLoadsEnabled) return;

    const timeoutId = window.setTimeout(() => {
      setAuxiliaryLoadsEnabled(true);
    }, 1200);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [auxiliaryLoadsEnabled, deferAuxiliaryLoads]);

  const activeScope = useMemo(() => {
    const params = new URLSearchParams(searchParams?.toString() || '');
    return scopeFromSession(params, session);
  }, [searchParams, session]);

  const scopeControls = useMemo(() => getScopeControls(session), [session]);
  const companies = useMemo(() => session?.product_companies || [], [session]);
  const visibleBranches = useMemo(
    () => getVisibleBranches(branches, branchSearch) as BranchOption[],
    [branchSearch, branches],
  );

  useEffect(() => {
    const nextBranchIds = uniqueBranchIds(activeScope.id_filiais || []);
    const selectionMode: 'all' | 'selected' =
      scopeControls.branchLocked ? 'selected' : (activeScope.branch_scope === 'all' ? 'all' : (nextBranchIds.length ? 'selected' : 'all'));
    setDraft({
      dt_ini: activeScope.dt_ini || '',
      dt_fim: activeScope.dt_fim || '',
      id_empresa: activeScope.id_empresa || '',
      id_filiais: selectionMode === 'all' ? [] : nextBranchIds,
      selectionMode,
    });
  }, [
    activeScope.dt_ini,
    activeScope.dt_fim,
    activeScope.id_empresa,
    (activeScope.id_filiais || []).join(','),
    activeScope.branch_scope,
    scopeControls.branchLocked,
  ]);

  useEffect(() => {
    if (typeof initialUnread === 'number') return;

    let active = true;
    const loadUnread = async () => {
      try {
        const qs = buildScopeSearchParams(activeScope).toString();
        const response = await apiGet(`/bi/notifications/unread-count${qs ? `?${qs}` : ''}`);
        if (active) setUnread(Number(response?.unread || 0));
      } catch {
        if (active) setUnread(0);
      }
    };

    loadUnread();
    return () => {
      active = false;
    };
  }, [activeScope, initialUnread]);

  const toggleAlerts = async () => {
    const next = !alertsOpen;
    setAlertsOpen(next);
    if (!next) return;
    setAlertsLoading(true);
    try {
      const qs = buildScopeSearchParams(activeScope).toString();
      const response = await apiGet(`/bi/notifications?unread_only=true&limit=12${qs ? `&${qs}` : ''}`);
      setAlerts(Array.isArray(response?.items) ? response.items : []);
      setUnread(Number(response?.unread || 0));
    } catch {
      setAlerts([]);
    } finally {
      setAlertsLoading(false);
    }
  };

  useEffect(() => {
    if (!auxiliaryLoadsEnabled) return;

    const companyId = draft.id_empresa || activeScope.id_empresa;
    if (!companyId) {
      setBranches([]);
      return;
    }

    let active = true;
    const loadBranches = async () => {
      setLoadingBranches(true);
      try {
        const response = await apiGet(`/bi/filiais?id_empresa=${companyId}`);
        const items = (response?.items || []) as BranchOption[];
        if (!active) return;
        setBranches(items);
        setDraft((current) => {
          if (scopeControls.branchLocked && session?.id_filial != null) {
            return {
              ...current,
              id_filiais: [String(session.id_filial)],
              selectionMode: 'selected',
            };
          }

          const allowedIds = new Set(items.map((item) => String(item.id_filial)));
          const filteredIds = current.id_filiais.filter((branchId) => allowedIds.has(branchId));
          const availableBranchIds = uniqueBranchIds(items.map((item) => item.id_filial));
          const allAvailableSelected = current.selectionMode === 'all'
            || (availableBranchIds.length > 0 && filteredIds.length === availableBranchIds.length);

          return {
            ...current,
            id_filiais: allAvailableSelected ? [] : filteredIds,
            selectionMode: allAvailableSelected ? 'all' : (filteredIds.length ? 'selected' : 'all'),
          };
        });
      } catch {
        if (active) setBranches([]);
      } finally {
        if (active) setLoadingBranches(false);
      }
    };

    loadBranches();
    return () => {
      active = false;
    };
  }, [activeScope.id_empresa, auxiliaryLoadsEnabled, draft.id_empresa, scopeControls.branchLocked, session?.id_filial]);

  // Let BrandingApplier follow the active company (multi-company users) so the
  // ambient background/logo switches without a full session reload.
  useEffect(() => {
    if (typeof window === 'undefined') return;
    const companyId = Number(activeScope.id_empresa || 0);
    if (!companyId) return;
    try {
      window.dispatchEvent(new CustomEvent('torqmind:company', { detail: companyId }));
    } catch {
      /* no-op */
    }
  }, [activeScope.id_empresa]);

  const currentUserLabel =
    userLabel ||
    session?.name ||
    session?.email ||
    (session?.user_role ? String(session.user_role) : undefined);

  const onLogout = () => {
    clearSessionCache();
    clearAuth();
    router.push('/');
  };

  const applyFilters = (overrides?: Partial<ScopeDraft>) => {
    const mergedDraft: ScopeDraft = {
      ...draft,
      ...(overrides || {}),
    };
    const validation = validateScopeDraft({
      branches,
      branchLocked: scopeControls.branchLocked,
      sessionBranchId: session?.id_filial,
      selectionMode: mergedDraft.selectionMode,
      selectedBranchIds: mergedDraft.id_filiais,
      dt_ini: mergedDraft.dt_ini,
      dt_fim: mergedDraft.dt_fim,
    });
    if (!validation.ok) return;
    const nextScope = buildValidatedScope({
      draft: mergedDraft,
      activeScope,
      effectiveBranchIds: validation.effectiveBranchIds,
      scopeEpoch: createScopeEpoch(),
    });
    const params = buildScopeSearchParams(nextScope);
    const query = params.toString();
    const nextUrl = query ? `${pathname}?${query}` : pathname;

    startScopeTransition(nextScope, pathname);
    prefetchProductScope(nextScope, router);
    startTransition(() => {
      router.replace(nextUrl);
    });
  };

  const navScope = scopeTransition.active && scopeTransition.scope
    ? {
        dt_ini: scopeTransition.scope.dt_ini || activeScope.dt_ini,
        dt_fim: scopeTransition.scope.dt_fim || activeScope.dt_fim,
        dt_ref: scopeTransition.scope.dt_ref || activeScope.dt_ref,
        scope_epoch: scopeTransition.scope.scope_epoch || activeScope.scope_epoch,
        id_empresa: scopeTransition.scope.id_empresa || activeScope.id_empresa,
        id_filial: scopeTransition.scope.id_filial || activeScope.id_filial,
        id_filiais: scopeTransition.scope.id_filiais || activeScope.id_filiais,
        branch_scope: scopeTransition.scope.branch_scope || activeScope.branch_scope,
      }
    : {
        dt_ini: activeScope.dt_ini,
        dt_fim: activeScope.dt_fim,
        dt_ref: activeScope.dt_ref,
        scope_epoch: activeScope.scope_epoch,
        id_empresa: activeScope.id_empresa,
        id_filial: activeScope.id_filial,
        id_filiais: activeScope.id_filiais,
        branch_scope: activeScope.branch_scope,
      };
  const applying = scopeTransition.active;

  const selectedCompanyLabel = companyLabel(session, activeScope.id_empresa || draft.id_empresa || null);
  // FASE 13: only show the company dropdown when the user can switch AND has more
  // than one company. A single-company owner/admin sees a fixed label instead of
  // a useless one-option dropdown (UX only — the API still enforces access).
  const showCompanySelector = scopeControls.canSwitchCompany && companies.length > 1;
  const selectedBranchLabel = branchSelectionLabel(
    branches,
    draft.id_filiais,
    draft.selectionMode,
    scopeControls.branchLocked,
  );
  const scopeValidation = useMemo(
    () =>
      validateScopeDraft({
        branches,
        branchLocked: scopeControls.branchLocked,
        sessionBranchId: session?.id_filial,
        selectionMode: draft.selectionMode,
        selectedBranchIds: draft.id_filiais,
        dt_ini: draft.dt_ini,
        dt_fim: draft.dt_fim,
      }),
    [
      branches,
      scopeControls.branchLocked,
      session?.id_filial,
      draft.selectionMode,
      draft.id_filiais,
      draft.dt_ini,
      draft.dt_fim,
    ],
  );

  const triggerAuxiliaryLoads = () => {
    if (!auxiliaryLoadsEnabled) {
      setAuxiliaryLoadsEnabled(true);
    }
  };

  const toggleBranch = (branchId: string) => {
    triggerAuxiliaryLoads();
    setDraft((current) => {
      if (scopeControls.branchLocked) return current;
      const availableBranchIds = uniqueBranchIds(branches.map((branch) => branch.id_filial));
      const currentIds = current.selectionMode === 'all' ? [] : current.id_filiais;
      const isSelected = currentIds.includes(branchId);
      const nextIds = isSelected
        ? currentIds.filter((item) => item !== branchId)
        : uniqueBranchIds([...currentIds, branchId]);
      const allAvailableSelected = availableBranchIds.length > 0 && nextIds.length === availableBranchIds.length;

      return {
        ...current,
        id_filiais: allAvailableSelected ? [] : nextIds,
        selectionMode: allAvailableSelected ? 'all' : (nextIds.length ? 'selected' : 'all'),
      };
    });
  };

  const allBranchesChecked = !scopeControls.branchLocked && draft.selectionMode === 'all';

  const localScopeFallback = useMemo(() => buildBrowserLocalDefaultScope(session), [session]);
  const shortcutReferenceDateValue = localScopeFallback.dt_ref || formatBusinessCalendarDate(new Date());
  const shortcutReferenceDate = parseCalendarDate(shortcutReferenceDateValue) || new Date();
  const quickShortcutRanges = useMemo(
    () => buildQuickShortcutRanges(shortcutReferenceDate),
    [shortcutReferenceDateValue],
  );

  const activeQuickShortcut = quickShortcutRanges.find(
    (shortcut) => shortcut.range[0] === draft.dt_ini && shortcut.range[1] === draft.dt_fim,
  );
  const isCustomShortcut = !activeQuickShortcut && Boolean(draft.dt_ini || draft.dt_fim);
  const activeQuickShortcutId = activeQuickShortcut?.id;

  const applyQuickShortcut = (shortcutId: string) => {
    const shortcut = quickShortcutRanges.find((entry) => entry.id === shortcutId);
    if (!shortcut) return;
    setDraft((current) => ({
      ...current,
      dt_ini: shortcut.range[0],
      dt_fim: shortcut.range[1],
    }));
  };

  return (
    <>
      <header ref={headerRef} className={`productTopNav${navHidden || topNavCollapsed ? ' navHidden' : ''}`}>
        <div className="productTopBar">
          <div className="productBrand productBrandInline">
            <Image src="/brand/Logo_Icone.png" alt="TorqMind" width={34} height={34} priority />
            <div>
              <div className="productEyebrow">Plataforma Operacional</div>
              <div className="productTopTitle">{title}</div>
            </div>
          </div>

          <nav className="productTopLinks" aria-label="Navegação principal do produto">
            {filterProductNavGroups(session?.allowed_screens).map((group) => {
              const active = groupIsActive(pathname, group.children);
              const open = openFlyout === group.id;
              return (
                <div
                  key={group.id}
                  className={`productNavGroup${active ? ' is-active' : ''}${open ? ' is-open' : ''}`}
                  onMouseEnter={() => {
                    if (flyoutCloseTimer.current) clearTimeout(flyoutCloseTimer.current);
                    setOpenFlyout(group.id);
                  }}
                  onMouseLeave={() => {
                    flyoutCloseTimer.current = setTimeout(() => setOpenFlyout(null), 160);
                  }}
                >
                  <button
                    type="button"
                    className={`productTopLink productNavGroupBtn${active ? ' productTopLinkActive' : ''}`}
                    aria-expanded={open}
                    aria-haspopup="menu"
                    onClick={() => setOpenFlyout((cur) => (cur === group.id ? null : group.id))}
                    onFocus={() => setOpenFlyout(group.id)}
                  >
                    {group.label}
                    <span className="productNavCaret" aria-hidden>
                      ▾
                    </span>
                  </button>
                  {open ? (
                    <div className="productFlyout" role="menu">
                      {group.children.map((item) => {
                        const itemActive = linkIsActive(pathname, searchParams, item.path);
                        return (
                          <Link
                            key={`${group.id}-${item.path}`}
                            href={buildProductHref(item.path, navScope)}
                            className={`productFlyoutLink${itemActive ? ' is-active' : ''}`}
                            role="menuitem"
                            onClick={() => setOpenFlyout(null)}
                          >
                            {item.label}
                          </Link>
                        );
                      })}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </nav>

          <div className="productTopActions">
            <ThemeToggleButton />
            <button
              type="button"
              className="btn"
              aria-label={topNavCollapsed ? 'Mostrar menu superior' : 'Ocultar menu superior'}
              title={topNavCollapsed ? 'Mostrar menu superior' : 'Ocultar menu superior'}
              onClick={() => setTopNavCollapsed((v) => !v)}
            >
              {topNavCollapsed ? '☰' : '⌃'}
            </button>
            {unread > 0 ? (
              <div className="alertsMenu">
                <button
                  type="button"
                  className="pill alertsPill"
                  aria-expanded={alertsOpen}
                  onClick={() => void toggleAlerts()}
                >
                  Alertas {unread}
                </button>
                {alertsOpen ? (
                  <div className="alertsDropdown card">
                    {alertsLoading ? (
                      <div className="muted" style={{ fontSize: 12 }}>Carregando…</div>
                    ) : !alerts.length ? (
                      <div className="muted" style={{ fontSize: 12 }}>Sem alertas não lidos.</div>
                    ) : (
                      <ul className="alertsList">
                        {alerts.map((item: any) => (
                          <li key={item.id || item.notification_id || item.title}>
                            <strong>{item.title || item.severity || 'Alerta'}</strong>
                            <span className="muted">{item.message || item.body || item.detail || ''}</span>
                          </li>
                        ))}
                      </ul>
                    )}
                  </div>
                ) : null}
              </div>
            ) : null}
            {currentUserLabel ? <div className="pill productUserPill">{currentUserLabel}</div> : null}

            <div className="productTopSecondary" aria-label="Atalhos da conta">
              {session?.access?.platform ? (
                <Link className="btn productSecondaryBtn" href="/platform">
                  Plataforma
                </Link>
              ) : null}
              <Link className="btn productSecondaryBtn" href="/settings">
                Configurações
              </Link>
              <Link className="btn productSecondaryBtn" href="/security">
                Minha Segurança
              </Link>
            </div>

            <details className="productAccountMenu">
              <summary className="btn productAccountMenuSummary" aria-label="Menu da conta">
                Conta
              </summary>
              <div className="productAccountMenuPanel" role="menu">
                {session?.access?.platform ? (
                  <Link className="productAccountMenuLink" href="/platform" role="menuitem">
                    Plataforma
                  </Link>
                ) : null}
                <Link className="productAccountMenuLink" href="/settings" role="menuitem">
                  Configurações
                </Link>
                <Link className="productAccountMenuLink" href="/security" role="menuitem">
                  Minha Segurança
                </Link>
              </div>
            </details>

            <button className="btn" onClick={onLogout} aria-label="Sair da conta">
              Sair
            </button>
          </div>
        </div>
      </header>

      {topNavCollapsed ? (
        <button
          type="button"
          className="btn topNavReveal"
          aria-label="Mostrar menu superior"
          title="Mostrar menu superior"
          onClick={() => setTopNavCollapsed(false)}
        >
          ☰ Menu
        </button>
      ) : null}

      <aside className={`productSidebar${filterDockCollapsed ? ' is-collapsed' : ''}`}>
        <div className="productSidebarDockBar">
          <button
            type="button"
            className="btn productDockToggle"
            aria-expanded={!filterDockCollapsed}
            aria-label={filterDockCollapsed ? 'Expandir filtros' : 'Recolher filtros'}
            title={filterDockCollapsed ? 'Expandir filtros' : 'Recolher filtros'}
            onClick={() => setFilterDockCollapsed((v) => !v)}
          >
            {filterDockCollapsed ? '»' : '«'}
          </button>
          {!filterDockCollapsed ? (
            <span className="productDockLabel">Filtros</span>
          ) : (
            <span className="productDockCollapsedHint">Filtros</span>
          )}
        </div>

        {!filterDockCollapsed ? (
          <>
        <div className="productSidebarHeader">
          <div className="productEyebrow">Contexto operacional</div>
          <div className="productBrandTitle">{title}</div>
          <div className="muted">
            {applying
              ? 'Atualizando os dados antes de liberar os números finais.'
              : 'Os filtros abaixo atualizam a rota atual do produto sem sair do módulo.'}
          </div>
        </div>

        <div className={`productSidebarSection productFilters${hideScopeOnMobile ? ' scopeHiddenOnMobile' : ''}`}>
          <div className="productSectionLabel">Empresa e filiais</div>

          <label className="productField">
            <span>Empresa</span>
            {showCompanySelector ? (
              <select
                className="input"
                value={draft.id_empresa}
                onFocus={triggerAuxiliaryLoads}
                onChange={(event) => {
                  triggerAuxiliaryLoads();
                  setDraft((current) => ({
                    ...current,
                    id_empresa: event.target.value,
                    id_filiais: scopeControls.branchLocked && session?.id_filial != null ? [String(session.id_filial)] : [],
                    selectionMode: scopeControls.branchLocked ? 'selected' : 'all',
                  }));
                }}
              >
                {companies.map((item: any) => (
                  <option key={item.id_empresa} value={String(item.id_empresa)}>
                    {item.tenant_name || `Empresa ${item.id_empresa}`}
                  </option>
                ))}
              </select>
            ) : (
              <div className="productReadOnlyField">{selectedCompanyLabel}</div>
            )}
          </label>

          <div className="productField">
            <span>Filiais</span>
            <div className="productBranchPanel">
              {!scopeControls.branchLocked ? (
                <label className="productCheckboxRow">
                  <input
                    type="checkbox"
                    checked={allBranchesChecked}
                    disabled={!scopeControls.canSwitchBranch || loadingBranches}
                    onChange={(event) =>
                      setDraft((current) => ({
                        ...current,
                        selectionMode: event.target.checked ? 'all' : (current.id_filiais.length ? 'selected' : 'all'),
                        id_filiais: event.target.checked ? [] : current.id_filiais,
                      }))
                    }
                  />
                  <div>
                    <strong>Todas as filiais</strong>
                    <span>Usa a visão consolidada da empresa atual.</span>
                  </div>
                </label>
              ) : null}

              <input
                className="input productBranchSearchInput"
                type="search"
                value={branchSearch}
                placeholder="Buscar filial"
                aria-label="Buscar filiais"
                autoComplete="off"
                disabled={loadingBranches || !branches.length}
                onFocus={triggerAuxiliaryLoads}
                onChange={(event) => setBranchSearch(event.target.value)}
              />

              <div className={`productBranchChecklist ${allBranchesChecked ? 'is-muted' : ''}`}>
                {loadingBranches ? <div className="muted">Carregando filiais...</div> : null}
                {!loadingBranches && !branches.length ? <div className="muted">Nenhuma filial disponível para esta empresa.</div> : null}
                {!loadingBranches && !!branches.length && !visibleBranches.length ? (
                  <div className="muted">Nenhuma filial encontrada para essa busca.</div>
                ) : null}
                {visibleBranches.map((branch) => {
                  const branchId = String(branch.id_filial);
                  const checked = scopeControls.branchLocked
                    ? branchId === String(session?.id_filial ?? '')
                    : draft.selectionMode === 'selected' && draft.id_filiais.includes(branchId);

                  return (
                    <label key={branch.id_filial} className="productCheckboxRow">
                      <input
                        type="checkbox"
                        checked={checked}
                        disabled={
                          loadingBranches
                          || !scopeControls.canSwitchBranch
                          || (scopeControls.branchLocked && branchId !== String(session?.id_filial ?? ''))
                        }
                        onChange={() => toggleBranch(branchId)}
                      />
                      <div>
                        <strong>{branch.nome}</strong>
                        <span>Escopo operacional disponível para esta empresa.</span>
                      </div>
                    </label>
                  );
                })}
              </div>
            </div>
          </div>
        </div>

        <div className="productSidebarSection productFilters">
          <div className="productSectionLabel">Período</div>

          <div className="productDateGrid">
            <label className="productField">
              <span>De</span>
              <input
                className="input productDateInput"
                type="date"
                value={draft.dt_ini}
                onChange={(event) => setDraft((current) => ({ ...current, dt_ini: event.target.value }))}
              />
            </label>
            <label className="productField">
              <span>Até</span>
              <input
                className="input productDateInput"
                type="date"
                value={draft.dt_fim}
                onChange={(event) => setDraft((current) => ({ ...current, dt_fim: event.target.value }))}
              />
            </label>
          </div>
          <div className="productDateShortcuts">
            {quickShortcutRanges.map((shortcut) => (
              <button
                key={shortcut.id}
                type="button"
                className={`dateShortcutButton${activeQuickShortcutId === shortcut.id ? ' is-active' : ''}`}
                onClick={() => applyQuickShortcut(shortcut.id)}
              >
                {shortcut.label}
              </button>
            ))}
            <button
              type="button"
              className={`dateShortcutButton${isCustomShortcut ? ' is-active' : ''}`}
              onClick={() => setDraft((current) => ({ ...current }))}
            >
              Personalizado
            </button>
          </div>

          <div className="productScopeMeta">
            <div>
              <strong>Referência da leitura</strong>
              <span>{describeServerBaseDate(navScope.dt_ref || localScopeFallback.dt_ref)}</span>
            </div>
            <div>
              <strong>{applying ? 'Novo escopo' : 'Escopo atual'}</strong>
              <span>{selectedBranchLabel}</span>
            </div>
          </div>
          {!scopeValidation.ok ? (
            <div className="muted" style={{ color: 'var(--color-negative)' }} aria-live="polite">
              {scopeValidation.error}
            </div>
          ) : null}

          <button
            type="button"
            className="btn productApplyButton"
            onClick={() => applyFilters()}
            disabled={!scopeValidation.ok || applying}
          >
            {applying ? 'Aplicando...' : 'Aplicar filtros'}
          </button>
        </div>
          </>
        ) : null}
      </aside>
    </>
  );
}
