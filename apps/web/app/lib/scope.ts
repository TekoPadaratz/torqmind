'use client';

import { useMemo } from 'react';
import { useSearchParams } from 'next/navigation';

import { buildBrowserLocalDefaultScope } from './local-scope-defaults.mjs';
import { buildScopeSearchParams, readScopeFromSearch } from './product-scope.mjs';
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
  ready: boolean;
};

export function useScopeQuery(fallback?: Partial<ScopeQuery>): ScopeQuery {
  const searchParams = useSearchParams();
  const cachedSession = readCachedSession();

  return useMemo(() => {
    const sessionFallback = buildBrowserLocalDefaultScope(cachedSession);
    const scope = readScopeFromSearch(searchParams, {
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
    cachedSession,
    fallback,
    searchParams,
  ]);
}

export function buildScopeParams(scope: Partial<ScopeQuery>, options?: { includeDtRef?: boolean }): URLSearchParams {
  return buildScopeSearchParams(scope, options || {});
}
