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

    return {
      dt_ini: scope.dt_ini || '',
      dt_fim: scope.dt_fim || '',
      dt_ref: scope.dt_ref || '',
      scope_epoch: scope.scope_epoch || '',
      scope_key: scope.scope_key || '',
      id_filial: scope.id_filial || (id_filiais.length === 1 ? id_filiais[0] : null),
      id_filiais,
      id_filiais_key: id_filiais.join(','),
      id_empresa: scope.id_empresa || null,
      ready: true,
    };
  }, [
    fallbackDependency,
    search,
    sessionDependency,
  ]);
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

    const currentHref = search ? `${pathname}?${search}` : pathname;
    if (!needsCanonicalScope(currentHref)) return;

    const nextHref = buildCanonicalProductHref(currentHref, cachedSession, {
      scopeEpoch: createScopeEpoch(),
    });
    if (nextHref === currentHref) return;

    router.replace(nextHref);
  }, [pathname, router, search, sessionDependency]);
}

export function buildScopeParams(scope: Partial<ScopeQuery>, options?: { includeDtRef?: boolean }): URLSearchParams {
  return buildScopeSearchParams(scope, options || {});
}
