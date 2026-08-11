'use client';

import { useEffect, useMemo } from 'react';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';

import { buildBrowserLocalDefaultScope } from './local-scope-defaults.mjs';
import {
  buildCanonicalProductHref,
  buildScopeSearchParams,
  createScopeEpoch,
  needsCanonicalScope,
  readScopeFromSearch,
} from './product-scope.mjs';
import { readCachedSession } from './session';

export type ScopeQuery = {
  dt_ini: string;
  dt_fim: string;
  dt_ref: string;
  scope_epoch: string;
  scope_key: string;
  id_filial: string | null;
  id_filiais: string[];
  id_filiais_key: string;
  id_empresa: string | null;
  branch_scope?: "all" | "selected" | string;
  ready: boolean;
};

function buildScopeSessionDependency(session: any): string {
  return JSON.stringify({
    id_empresa: session?.id_empresa ?? null,
    id_filial: session?.id_filial ?? null,
    tenant_ids: Array.isArray(session?.tenant_ids) ? session.tenant_ids : [],
    product_companies: Array.isArray(session?.product_companies)
      ? session.product_companies.map((row: any) => row?.id_empresa ?? null)
      : [],
    default_scope: session?.default_scope ?? null,
    accesses: Array.isArray(session?.accesses)
      ? session.accesses.map((access: any) => ({
          id_empresa: access?.id_empresa ?? null,
          id_filial: access?.id_filial ?? null,
        }))
      : [],
  });
}

export function useScopeQuery(fallback?: Partial<ScopeQuery>): ScopeQuery {
  const searchParams = useSearchParams();
  const search = searchParams?.toString() || '';
  const cachedSession = readCachedSession();
  const sessionDependency = buildScopeSessionDependency(cachedSession);
  const fallbackDependency = JSON.stringify(fallback || {});

  return useMemo(() => {
    const sessionFallback = buildBrowserLocalDefaultScope(cachedSession);
    const scope = readScopeFromSearch(new URLSearchParams(search), {
      ...sessionFallback,
      ...(fallback || {}),
    });
    const id_filiais = scope.id_filiais || [];
    const branch_scope = String(scope.branch_scope || '').trim().toLowerCase();
    // branch_scope=all não serializa id_filiais na URL — reexpande do fallback
    // da sessão para telas que precisam de lista concreta (grids multi-filial).
    const effectiveFiliais =
      id_filiais.length > 0
        ? id_filiais
        : branch_scope === 'all'
          ? (sessionFallback.id_filiais || [])
          : [];

    return {
      dt_ini: scope.dt_ini || '',
      dt_fim: scope.dt_fim || '',
      dt_ref: scope.dt_ref || '',
      scope_epoch: scope.scope_epoch || '',
      scope_key: scope.scope_key || '',
      id_filial: scope.id_filial || (effectiveFiliais.length === 1 ? effectiveFiliais[0] : null),
      id_filiais: effectiveFiliais,
      id_filiais_key: effectiveFiliais.join(','),
      id_empresa: scope.id_empresa || null,
      branch_scope: branch_scope || undefined,
      ready: true,
    };
  }, [
    fallbackDependency,
    search,
    sessionDependency,
  ]);
}

function sessionCanResolveEmpresa(session: any): boolean {
  if (!session) return false;
  if (session.id_empresa != null && String(session.id_empresa).trim() !== '') return true;
  if (session.default_scope?.id_empresa != null && String(session.default_scope.id_empresa).trim() !== '') {
    return true;
  }
  if (Array.isArray(session.tenant_ids) && session.tenant_ids.some((id: any) => Number(id) > 0)) {
    return true;
  }
  if (
    Array.isArray(session.product_companies)
    && session.product_companies.some((row: any) => Number(row?.id_empresa) > 0)
  ) {
    return true;
  }
  return false;
}

export function useEnsureScopedProductUrl(): void {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const search = searchParams?.toString() || '';
  const cachedSession = readCachedSession();
  const sessionDependency = buildScopeSessionDependency(cachedSession);

  useEffect(() => {
    if (!cachedSession || !pathname) return;
    // Sem empresa resolvível, qualquer replace só minta epoch e entra em loop
    // (flicker + storm de unread-count). Espera hidratar /auth/me.
    if (!sessionCanResolveEmpresa(cachedSession)) return;

    const currentHref = search ? `${pathname}?${search}` : pathname;
    if (!needsCanonicalScope(currentHref)) return;

    // Preserve epoch existente — nunca remarcar a cada effect.
    const existingEpoch = String(
      new URLSearchParams(search).get('scope_epoch') || '',
    ).trim();

    const nextHref = buildCanonicalProductHref(currentHref, cachedSession, {
      scopeEpoch: existingEpoch || createScopeEpoch(),
    });
    if (nextHref === currentHref) return;

    // Só navega quando o destino ficou canônico. URL incompleta nunca é escrita.
    if (needsCanonicalScope(nextHref)) return;

    router.replace(nextHref);
  }, [pathname, router, search, sessionDependency]);
}

export function buildScopeParams(scope: Partial<ScopeQuery>, options?: { includeDtRef?: boolean }): URLSearchParams {
  return buildScopeSearchParams(scope, options || {});
}
