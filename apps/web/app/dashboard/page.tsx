'use client';

import { useEffect } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';

import { buildProductHref } from '../lib/product-scope.mjs';
import { readScopeFromSearch } from '../lib/product-scope.mjs';

export const dynamic = 'force-dynamic';

/** Dashboard Geral removido da IA — redireciona para Vendas preservando escopo. */
export default function DashboardRedirectPage() {
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    const scope = readScopeFromSearch(searchParams, {});
    const href = buildProductHref('/sales', {
      dt_ini: scope.dt_ini,
      dt_fim: scope.dt_fim,
      dt_ref: scope.dt_ref,
      id_empresa: scope.id_empresa,
      id_filial: scope.id_filial,
      id_filiais: scope.id_filiais,
      branch_scope: scope.branch_scope,
      scope_epoch: scope.scope_epoch,
    });
    router.replace(href);
  }, [router, searchParams]);

  return (
    <main className="container" style={{ paddingTop: 80 }}>
      <p className="muted">Redirecionando para Vendas…</p>
    </main>
  );
}
