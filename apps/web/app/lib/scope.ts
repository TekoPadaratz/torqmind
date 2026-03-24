'use client';

import { useMemo } from 'react';
import { useSearchParams } from 'next/navigation';

import { buildScopeSearchParams, readScopeFromSearch } from './product-scope.mjs';

export type ScopeQuery = {
  dt_ini: string;
  dt_fim: string;
  dt_ref: string;
  id_filial: string | null;
  id_empresa: string | null;
  ready: boolean;
};

export function useScopeQuery(fallback?: Partial<ScopeQuery>): ScopeQuery {
  const searchParams = useSearchParams();

  return useMemo(() => {
    const scope = readScopeFromSearch(searchParams, fallback || {});
    return {
      dt_ini: scope.dt_ini || '',
      dt_fim: scope.dt_fim || '',
      dt_ref: scope.dt_ref || '',
      id_filial: scope.id_filial || null,
      id_empresa: scope.id_empresa || null,
      ready: true,
    };
  }, [fallback, searchParams]);
}

export function buildScopeParams(scope: Partial<ScopeQuery>, options?: { includeDtRef?: boolean }): URLSearchParams {
  return buildScopeSearchParams(scope, options || {});
}
