'use client';

import Image from 'next/image';
import Link from 'next/link';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { startTransition, useEffect, useMemo, useState } from 'react';

import { apiGet } from '../lib/api';
import { clearAuth } from '../lib/auth';
import { getVisibleBranches, uniqueBranchIds } from '../lib/branch-state.mjs';
import { buildQuickShortcutRanges, formatBusinessCalendarDate, parseCalendarDate } from '../lib/calendar-date.mjs';
import { buildBrowserLocalDefaultScope } from '../lib/local-scope-defaults.mjs';
import { describeLastSync, describeServerBaseDate, describeSyncMessage } from '../lib/reading-copy.mjs';
import { clearSessionCache, loadSession, readCachedSession } from '../lib/session';
import { buildValidatedScope, validateScopeDraft } from '../lib/scope-validation.mjs';
import { prefetchProductScope, startScopeTransition, useScopeTransitionState } from '../lib/scope-runtime';
import {
  PRODUCT_LINKS,
  buildProductHref,
  buildScopeSearchParams,
  createScopeEpoch,
  getScopeControls,
  readScopeFromSearch,
} from '../lib/product-scope.mjs';

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

  const fallbackCompany =
    parsed.id_empresa ||
    (fallback.id_empresa != null ? String(fallback.id_empresa) : null) ||
    (session?.id_empresa != null ? String(session.id_empresa) : null) ||
    (session?.tenant_ids?.length ? String(session.tenant_ids[0]) : null);

  const fallbackBranchIds = controls.branchLocked && session?.id_filial != null
    ? [String(session.id_filial)]
    : uniqueBranchIds([
        ...(parsed.id_filiais || []),
        ...(fallback.id_filiais || []),
        parsed.id_filial,
        fallback.id_filial,
        session?.id_filial,
      ]);

  return {
    dt_ini: parsed.dt_ini || fallback.dt_ini || '',
    dt_fim: parsed.dt_fim || fallback.dt_fim || '',
    dt_ref: parsed.dt_ref || fallback.dt_ref || '',
    scope_epoch: parsed.scope_epoch || fallback.scope_epoch || '',
    scope_key: parsed.scope_key || fallback.scope_key || '',
    id_empresa: fallbackCompany,
    id_filial: fallbackBranchIds.length === 1 ? fallbackBranchIds[0] : null,
    id_filiais: fallbackBranchIds,
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
  initialSyncStatus,
  deferAuxiliaryLoads = false,
}: {
  title: string;
  userLabel?: string;
  initialUnread?: number;
  initialSyncStatus?: any;
  deferAuxiliaryLoads?: boolean;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const [session, setSession] = useState<any>(readCachedSession());
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
  const [syncStatus, setSyncStatus] = useState<any>(initialSyncStatus ?? null);
  const [auxiliaryLoadsEnabled, setAuxiliaryLoadsEnabled] = useState(!deferAuxiliaryLoads);
  const scopeTransition = useScopeTransitionState();

  useEffect(() => {
    document.body.classList.add('product-shell');
    return () => document.body.classList.remove('product-shell');
  }, []);

  useEffect(() => {
    let active = true;

    const hydrateSession = async () => {
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

  useEffect(() => {
    if (initialSyncStatus !== undefined) {
      setSyncStatus(initialSyncStatus || null);
    }
  }, [initialSyncStatus]);

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
    setDraft({
      dt_ini: activeScope.dt_ini || '',
      dt_fim: activeScope.dt_fim || '',
      id_empresa: activeScope.id_empresa || '',
      id_filiais: nextBranchIds,
      selectionMode: scopeControls.branchLocked || nextBranchIds.length ? 'selected' : 'all',
    });
  }, [
    activeScope.dt_ini,
    activeScope.dt_fim,
    activeScope.id_empresa,
    (activeScope.id_filiais || []).join(','),
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

  useEffect(() => {
    if (initialSyncStatus?.available === true) return;
    if (!auxiliaryLoadsEnabled) return;

    const companyId = activeScope.id_empresa || session?.id_empresa;
    if (!companyId) {
      setSyncStatus(null);
      return;
    }

    let active = true;
    const loadSyncStatus = async () => {
      try {
        const params = buildScopeSearchParams({
          id_empresa: companyId,
          id_filial: activeScope.id_filial,
          id_filiais: activeScope.id_filiais,
        }).toString();
        const response = await apiGet(`/bi/sync/status${params ? `?${params}` : ''}`);
        if (active) setSyncStatus(response);
      } catch {
        if (active) {
          setSyncStatus({
            available: false,
            last_sync_at: null,
            message: 'Não foi possível consultar a última sincronização agora.',
          });
        }
      }
    };

    loadSyncStatus();
    return () => {
      active = false;
    };
  }, [
    activeScope.id_empresa,
    activeScope.id_filial,
    activeScope.id_filiais.join(','),
    auxiliaryLoadsEnabled,
    initialSyncStatus,
    session?.id_empresa,
  ]);

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
          return {
            ...current,
            id_filiais: filteredIds,
            selectionMode: filteredIds.length ? 'selected' : 'all',
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
      }
    : {
        dt_ini: activeScope.dt_ini,
        dt_fim: activeScope.dt_fim,
        dt_ref: activeScope.dt_ref,
        scope_epoch: activeScope.scope_epoch,
        id_empresa: activeScope.id_empresa,
        id_filial: activeScope.id_filial,
        id_filiais: activeScope.id_filiais,
      };
  const applying = scopeTransition.active;

  const selectedCompanyLabel = companyLabel(session, activeScope.id_empresa || draft.id_empresa || null);
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
      const isSelected = current.id_filiais.includes(branchId);
      const nextIds = isSelected
        ? current.id_filiais.filter((item) => item !== branchId)
        : uniqueBranchIds([...current.id_filiais, branchId]);

      return {
        ...current,
        id_filiais: nextIds,
        selectionMode: nextIds.length ? 'selected' : 'all',
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
      <header className="productTopNav">
        <div className="productTopBar">
          <div className="productBrand productBrandInline">
            <Image src="/brand/Logo_Icone.png" alt="TorqMind" width={34} height={34} priority />
            <div>
              <div className="productEyebrow">TorqMind BI</div>
              <div className="productTopTitle">{title}</div>
            </div>
          </div>

          <nav className="productTopLinks" aria-label="Navegação principal do produto">
            {PRODUCT_LINKS.map((item) => {
              const isActive = pathname === item.path;
              return (
                <Link
                  key={item.path}
                  href={buildProductHref(item.path, navScope)}
                  className={`productTopLink ${isActive ? 'productTopLinkActive' : ''}`}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>

          <div className="productTopActions">
            <span className="pill">Alertas {unread}</span>
            {currentUserLabel ? <div className="pill productUserPill">{currentUserLabel}</div> : null}
            {session?.access?.platform ? (
              <Link className="btn" href="/platform">
                Platform
              </Link>
            ) : null}
            <button className="btn" onClick={onLogout} aria-label="Sair da conta">
              Sair
            </button>
          </div>
        </div>
      </header>

      <aside className="productSidebar">
        <div className="productSidebarHeader">
          <div className="productEyebrow">Contexto operacional</div>
          <div className="productBrandTitle">{title}</div>
          <div className="muted">
            {applying
              ? 'Atualizando o novo recorte antes de liberar os números finais.'
              : 'Os filtros abaixo atualizam a rota atual do produto sem sair do módulo.'}
          </div>
        </div>

        <div className="productSidebarSection productFilters">
          <div className="productSectionLabel">Empresa e filiais</div>

          <label className="productField">
            <span>Empresa</span>
            {scopeControls.canSwitchCompany ? (
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
            <div className="muted" style={{ color: '#fca5a5' }} aria-live="polite">
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

        <div className="productSidebarSection productSyncSection">
          <div className="productSectionLabel">Frescor operacional</div>
          <div className="productSyncValue">{describeLastSync(syncStatus)}</div>
          <div className="muted">
            {describeSyncMessage(syncStatus)}
          </div>
        </div>
      </aside>
    </>
  );
}
