'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';

import AppNav from '../components/AppNav';
import { apiGet } from '../lib/api';
import { clearAuth } from '../lib/auth';
import { extractApiError } from '../lib/errors';
import { loadSession } from '../lib/session';

export const dynamic = 'force-dynamic';

function todayISO() {
  const d = new Date();
  return d.toISOString().slice(0, 10);
}

function daysAgoISO(days: number) {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
}

export default function ScopePage() {
  const router = useRouter();

  const [claims, setClaims] = useState<any>(null);
  const [dtIni, setDtIni] = useState(daysAgoISO(30));
  const [dtFim, setDtFim] = useState(todayISO());
  const [dtRef, setDtRef] = useState(todayISO());

  const [idEmpresa, setIdEmpresa] = useState<string>('1');
  const [filiais, setFiliais] = useState<any[]>([]);
  const [idFilial, setIdFilial] = useState<string>(''); // empty = all
  const [loading, setLoading] = useState<boolean>(true);
  const [err, setErr] = useState<string>('');

  const userLabel = useMemo(() => {
    if (!claims) return undefined;
    const role = claims.user_role || claims.role;
    const emp = claims.id_empresa ? `E${claims.id_empresa}` : '';
    const fil = claims.id_filial ? `F${claims.id_filial}` : '';
    const parts = [role, emp, fil].filter(Boolean);
    return parts.join(' · ');
  }, [claims]);

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      setErr('');
      try {
        const me = await loadSession(router, 'product');
        if (!me) return;
        const canSelectCrossTenant = me.user_role === 'platform_master' || me.user_role === 'product_global';
        if (!canSelectCrossTenant) {
          router.replace(me.home_path || '/dashboard');
          return;
        }
        setClaims(me);

        setIdEmpresa(String(me.id_empresa || me.tenant_ids?.[0] || 1));

        if (me.id_filial) {
          setIdFilial(String(me.id_filial));
        }
      } catch (e: any) {
        clearAuth();
        setErr(extractApiError(e, 'Sessão inválida. Faça login novamente.'));
        router.push('/');
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [router]);

  // Load filiais when tenant changes
  useEffect(() => {
    const loadFiliais = async () => {
      if (!claims || (claims.user_role !== 'platform_master' && claims.user_role !== 'product_global')) return;
      try {
        const qs = new URLSearchParams();
        qs.set('id_empresa', idEmpresa);
        const res = await apiGet(`/bi/filiais?${qs.toString()}`);
        setFiliais(res.items || []);
      } catch (e) {
        console.error(e);
        setFiliais([]);
      }
    };
    loadFiliais();
  }, [claims, idEmpresa]);

  const apply = () => {
    const qs = new URLSearchParams({ dt_ini: dtIni, dt_fim: dtFim, dt_ref: dtRef || dtFim });

    // id_filial empty means "all" for MASTER/OWNER
    if (idFilial) qs.set('id_filial', idFilial);

    // MASTER can navigate across tenants
    if (claims?.user_role === 'platform_master' || claims?.user_role === 'product_global') qs.set('id_empresa', idEmpresa || '1');

    router.push(`/dashboard?${qs.toString()}`);
  };

  const canPickEmpresa = claims?.user_role === 'platform_master' || claims?.user_role === 'product_global';
  const canPickFilial = claims?.user_role !== 'tenant_manager' || !(claims?.id_filial);

  return (
    <div>
      <AppNav title="Definir Escopo" userLabel={userLabel} />

      <div className="container">
        <div className="card" style={{ maxWidth: 720, margin: '30px auto' }}>
          <h1>Escopo do BI</h1>
          <div className="muted">Defina o período e a unidade.</div>

          <div style={{ height: 16 }} />

          {loading ? <p>Carregando...</p> : null}
          {err ? <p style={{ color: '#fb7185' }}>{err}</p> : null}

          <div className="row" style={{ gap: 12, alignItems: 'flex-end' }}>
            <div style={{ flex: 1 }}>
              <div className="label">Data inicial</div>
              <input className="input" type="date" value={dtIni} onChange={(e) => setDtIni(e.target.value)} />
            </div>
            <div style={{ flex: 1 }}>
              <div className="label">Data final</div>
              <input className="input" type="date" value={dtFim} onChange={(e) => setDtFim(e.target.value)} />
            </div>
            <div style={{ flex: 1 }}>
              <div className="label">Data de referência</div>
              <input className="input" type="date" value={dtRef} onChange={(e) => setDtRef(e.target.value)} />
            </div>
          </div>

          <div style={{ height: 12 }} />

          <div className="row" style={{ gap: 12, alignItems: 'flex-end' }}>
            <div style={{ flex: 1 }}>
              <div className="label">Empresa</div>
              <input
                className="input"
                value={idEmpresa}
                onChange={(e) => setIdEmpresa(e.target.value)}
                disabled={!canPickEmpresa}
                placeholder="id_empresa"
              />
              <div className="muted">{!canPickEmpresa ? 'Fixado pelo seu login' : 'Empresa em análise'}</div>
            </div>

            <div style={{ flex: 1 }}>
              <div className="label">Filial</div>
              <select
                className="input"
                value={idFilial}
                onChange={(e) => setIdFilial(e.target.value)}
                disabled={!canPickFilial}
              >
                {canPickFilial ? <option value="">Todas</option> : null}
                {filiais.map((f) => (
                  <option key={f.id_filial} value={String(f.id_filial)}>
                    {f.id_filial} — {f.nome}
                  </option>
                ))}
              </select>
              <div className="muted">{claims?.user_role === 'tenant_manager' ? 'Fixado pelo seu login' : 'Visão por unidade'}</div>
            </div>
          </div>

          <div style={{ height: 16 }} />

          <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center' }}>
            <button className="btn" onClick={apply}>
              Aplicar escopo
            </button>
            <div className="muted">Escopo aplicado em todos os painéis.</div>
          </div>
        </div>
      </div>
    </div>
  );
}
