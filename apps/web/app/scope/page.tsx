'use client';

import { useEffect } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';

import { buildScopeSearchParams } from '../lib/product-scope.mjs';
import { loadSession } from '../lib/session';

export const dynamic = 'force-dynamic';

export default function ScopePage() {
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    const redirect = async () => {
      const me = await loadSession(router, 'product');
      if (!me) return;

      const params = new URLSearchParams(searchParams?.toString() || '');
      const hasRange = Boolean(params.get('dt_ini') && params.get('dt_fim'));
      if (hasRange) {
        const redirected = buildScopeSearchParams({
          dt_ini: params.get('dt_ini'),
          dt_fim: params.get('dt_fim'),
          id_empresa: params.get('id_empresa') || me?.default_scope?.id_empresa,
          id_filial: params.get('id_filial') || me?.default_scope?.id_filial,
        });
        router.replace(`/sales?${redirected.toString()}`);
        return;
      }

      router.replace(me?.home_path || '/sales');
    };

    redirect();
  }, [router, searchParams]);

  return (
    <div className="container">
      <div className="card">Redirecionando para Vendas…</div>
    </div>
  );
}
