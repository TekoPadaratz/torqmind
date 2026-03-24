'use client';

import Image from 'next/image';
import Link from 'next/link';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';
import { useEffect, useMemo, useState } from 'react';

import { apiGet } from '../lib/api';
import { clearAuth } from '../lib/auth';
import {
  PRODUCT_LINKS,
  buildProductHref,
  buildScopeSearchParams,
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
  id_filial: string;
};

function scopeFromSession(searchParams: URLSearchParams, session: any) {
  const fallback = session?.default_scope || {};
  const parsed = readScopeFromSearch(searchParams, fallback);
  const fallbackCompany =
    parsed.id_empresa ||
    (fallback.id_empresa != null ? String(fallback.id_empresa) : null) ||
    (session?.id_empresa != null ? String(session.id_empresa) : null) ||
    (session?.tenant_ids?.length ? String(session.tenant_ids[0]) : null);
  const controls = getScopeControls(session);
  const fallbackBranch =
    controls.branchLocked && session?.id_filial != null
      ? String(session.id_filial)
      : parsed.id_filial ||
        (fallback.id_filial != null ? String(fallback.id_filial) : null) ||
        (session?.id_filial != null ? String(session.id_filial) : null);

  return {
    dt_ini: parsed.dt_ini || fallback.dt_ini || '',
    dt_fim: parsed.dt_fim || fallback.dt_fim || '',
    dt_ref: session?.server_today || fallback.server_today || parsed.dt_ref || fallback.dt_ref || '',
    id_empresa: fallbackCompany,
    id_filial: fallbackBranch,
  };
}

function companyLabel(session: any, idEmpresa: string | null) {
  if (!idEmpresa) return 'Empresa não definida';
  const companies = session?.product_companies || [];
  const match = companies.find((item: any) => String(item.id_empresa) === String(idEmpresa));
  if (!match) return `Empresa ${idEmpresa}`;
  return `${match.tenant_name || `Empresa ${idEmpresa}`}`;
}

export default function AppNav({
  title,
  userLabel,
  initialUnread,
}: {
  title: string;
  userLabel?: string;
  initialUnread?: number;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const [session, setSession] = useState<any>(null);
  const [draft, setDraft] = useState<ScopeDraft>({
    dt_ini: '',
    dt_fim: '',
    id_empresa: '',
    id_filial: '',
  });
  const [branches, setBranches] = useState<BranchOption[]>([]);
  const [loadingBranches, setLoadingBranches] = useState(false);
  const [unread, setUnread] = useState(initialUnread ?? 0);

  useEffect(() => {
    document.body.classList.add('product-shell');
    return () => document.body.classList.remove('product-shell');
  }, []);

  useEffect(() => {
    const loadSession = async () => {
      try {
        const me = await apiGet('/auth/me');
        setSession(me);
      } catch {
        setSession(null);
      }
    };
    loadSession();
  }, []);

  useEffect(() => {
    if (typeof initialUnread === 'number') {
      setUnread(initialUnread);
    }
  }, [initialUnread]);

  const activeScope = useMemo(() => {
    const params = new URLSearchParams(searchParams?.toString() || '');
    return scopeFromSession(params, session);
  }, [searchParams, session]);

  const scopeControls = useMemo(() => getScopeControls(session), [session]);
  const companies = useMemo(() => session?.product_companies || [], [session]);

  useEffect(() => {
    setDraft({
      dt_ini: activeScope.dt_ini || '',
      dt_fim: activeScope.dt_fim || '',
      id_empresa: activeScope.id_empresa || '',
      id_filial: activeScope.id_filial || '',
    });
  }, [activeScope.dt_ini, activeScope.dt_fim, activeScope.id_empresa, activeScope.id_filial]);

  useEffect(() => {
    const loadUnread = async () => {
      if (typeof initialUnread === 'number') return;
      try {
        const qs = buildScopeSearchParams(activeScope).toString();
        const response = await apiGet(`/bi/notifications/unread-count${qs ? `?${qs}` : ''}`);
        setUnread(Number(response?.unread || 0));
      } catch {
        setUnread(0);
      }
    };
    loadUnread();
  }, [activeScope, initialUnread]);

  useEffect(() => {
    const companyId = draft.id_empresa || activeScope.id_empresa;
    if (!companyId) {
      setBranches([]);
      return;
    }

    const loadBranches = async () => {
      setLoadingBranches(true);
      try {
        const response = await apiGet(`/bi/filiais?id_empresa=${companyId}`);
        const items = (response?.items || []) as BranchOption[];
        setBranches(items);
      } catch {
        setBranches([]);
      } finally {
        setLoadingBranches(false);
      }
    };

    loadBranches();
  }, [activeScope.id_empresa, draft.id_empresa]);

  useEffect(() => {
    if (!scopeControls.branchLocked) return;
    if (session?.id_filial == null) return;
    setDraft((current) => ({ ...current, id_filial: String(session.id_filial) }));
  }, [scopeControls.branchLocked, session]);

  const currentUserLabel =
    userLabel ||
    session?.name ||
    session?.email ||
    (session?.user_role ? String(session.user_role) : undefined);

  const onLogout = () => {
    clearAuth();
    router.push('/');
  };

  const applyFilters = () => {
    const params = buildScopeSearchParams({
      dt_ini: draft.dt_ini,
      dt_fim: draft.dt_fim,
      id_empresa: draft.id_empresa || activeScope.id_empresa,
      id_filial: draft.id_filial,
    });
    const query = params.toString();
    router.push(query ? `${pathname}?${query}` : pathname);
  };

  const navScope = {
    dt_ini: activeScope.dt_ini,
    dt_fim: activeScope.dt_fim,
    id_empresa: activeScope.id_empresa,
    id_filial: activeScope.id_filial,
  };

  const selectedCompanyLabel = companyLabel(session, activeScope.id_empresa || draft.id_empresa || null);
  const selectedBranchLabel = draft.id_filial
    ? branches.find((item) => String(item.id_filial) === String(draft.id_filial))?.nome || `Filial ${draft.id_filial}`
    : 'Todas as filiais';

  return (
    <aside className="productSidebar">
      <div className="productSidebarHeader">
        <div className="productBrand">
          <Image src="/brand/Logo_Icone.png" alt="TorqMind" width={34} height={34} priority />
          <div>
            <div className="productEyebrow">TorqMind BI</div>
            <div className="productBrandTitle">{title}</div>
          </div>
        </div>
        {currentUserLabel ? <div className="pill productUserPill">{currentUserLabel}</div> : null}
      </div>

      <div className="productSidebarSection">
        <div className="productSectionLabel">Navegação</div>
        <nav className="productNavLinks">
          {PRODUCT_LINKS.map((item) => {
            const isActive = pathname === item.path;
            return (
              <Link
                key={item.path}
                href={buildProductHref(item.path, navScope)}
                className={`productNavLink ${isActive ? 'productNavLinkActive' : ''}`}
              >
                <span>{item.label}</span>
              </Link>
            );
          })}
        </nav>
      </div>

      <div className="productSidebarSection productFilters">
        <div className="productSectionLabel">Filtros de produção</div>

        <label className="productField">
          <span>Empresa</span>
          {scopeControls.canSwitchCompany ? (
            <select
              className="input"
              value={draft.id_empresa}
              onChange={(event) =>
                setDraft((current) => ({
                  ...current,
                  id_empresa: event.target.value,
                  id_filial: scopeControls.branchLocked ? current.id_filial : '',
                }))
              }
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

        <label className="productField">
          <span>Filial</span>
          <select
            className="input"
            value={draft.id_filial}
            disabled={scopeControls.branchLocked || !scopeControls.canSwitchBranch || loadingBranches}
            onChange={(event) => setDraft((current) => ({ ...current, id_filial: event.target.value }))}
          >
            {!scopeControls.branchLocked ? <option value="">Todas as filiais</option> : null}
            {branches.map((item) => (
              <option key={item.id_filial} value={String(item.id_filial)}>
                {item.id_filial} - {item.nome}
              </option>
            ))}
          </select>
        </label>

        <div className="productDateGrid">
          <label className="productField">
            <span>De</span>
            <input
              className="input"
              type="date"
              value={draft.dt_ini}
              onChange={(event) => setDraft((current) => ({ ...current, dt_ini: event.target.value }))}
            />
          </label>
          <label className="productField">
            <span>Até</span>
            <input
              className="input"
              type="date"
              value={draft.dt_fim}
              onChange={(event) => setDraft((current) => ({ ...current, dt_fim: event.target.value }))}
            />
          </label>
        </div>

        <div className="productScopeMeta">
          <div>
            <strong>Base do servidor</strong>
            <span>{activeScope.dt_ref || session?.server_today || 'indisponível'}</span>
          </div>
          <div>
            <strong>Filial ativa</strong>
            <span>{selectedBranchLabel}</span>
          </div>
        </div>

        <button className="btn productApplyButton" onClick={applyFilters} disabled={!draft.dt_ini || !draft.dt_fim}>
          Aplicar filtros
        </button>
      </div>

      <div className="productSidebarSection productSidebarFooter">
        <div className="productSidebarMeta">
          <span className="pill">Alertas {unread}</span>
          {session?.access?.platform ? (
            <Link className="btn" href="/platform">
              Platform
            </Link>
          ) : null}
        </div>
        <button className="btn" onClick={onLogout} aria-label="Sair da conta">
          Sair
        </button>
      </div>
    </aside>
  );
}
