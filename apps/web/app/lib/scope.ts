'use client';

import { useEffect, useState } from 'react';

export type ScopeQuery = {
  dt_ini: string;
  dt_fim: string;
  dt_ref: string;
  id_filial: string | null;
  id_empresa: string | null;
  ready: boolean;
};

export function useScopeQuery(): ScopeQuery {
  const [scope, setScope] = useState<ScopeQuery>({
    dt_ini: '',
    dt_fim: '',
    dt_ref: '',
    id_filial: null,
    id_empresa: null,
    ready: false,
  });

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    setScope({
      dt_ini: params.get('dt_ini') || '',
      dt_fim: params.get('dt_fim') || '',
      dt_ref: params.get('dt_ref') || params.get('dt_fim') || '',
      id_filial: params.get('id_filial'),
      id_empresa: params.get('id_empresa'),
      ready: true,
    });
  }, []);

  return scope;
}
