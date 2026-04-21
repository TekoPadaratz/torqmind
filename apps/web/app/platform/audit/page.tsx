'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../../components/PlatformShell';
import { apiGet } from '../../lib/api';
import { formatDateTime } from '../../lib/format';
import { loadSession } from '../../lib/session';

export const dynamic = 'force-dynamic';

export default function PlatformAuditPage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [items, setItems] = useState<any[]>([]);
  const [selected, setSelected] = useState<any>(null);
  const [filters, setFilters] = useState({ entity_type: '', action: '', entity_id: '', date_from: '', date_to: '' });
  const [error, setError] = useState('');

  async function load(session: any, currentFilters = filters) {
    const query = new URLSearchParams({ limit: '200' });
    if (currentFilters.entity_type) query.set('entity_type', currentFilters.entity_type);
    if (currentFilters.action) query.set('action', currentFilters.action);
    if (currentFilters.entity_id) query.set('entity_id', currentFilters.entity_id);
    if (currentFilters.date_from) query.set('date_from', currentFilters.date_from);
    if (currentFilters.date_to) query.set('date_to', currentFilters.date_to);
    const res = await apiGet(`/platform/audit?${query.toString()}`);
    setItems(res?.items || []);
    if (selected) {
      const fresh = (res?.items || []).find((item: any) => item.id === selected.id);
      setSelected(fresh || null);
    }
  }

  useEffect(() => {
    const boot = async () => {
      const session = await loadSession(router, 'platform');
      if (!session) return;
      setMe(session);
      try {
        await load(session, filters);
      } catch (err: any) {
        setError(err?.message || 'Falha ao carregar auditoria.');
      }
    };
    boot();
  }, [router]);

  if (!me) return null;

  return (
    <PlatformShell
      title="Auditoria global"
      subtitle="Trilha central com filtros por ação, entidade, data e detalhes legíveis dos valores antigos e novos."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}

      <div className="platformGrid">
        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Filtro</div>
              <h2>Refinar eventos</h2>
            </div>
          </div>
          <div className="platformFormGrid">
            <input className="input" placeholder="entity_type" value={filters.entity_type} onChange={(e) => setFilters({ ...filters, entity_type: e.target.value })} />
            <input className="input" placeholder="action" value={filters.action} onChange={(e) => setFilters({ ...filters, action: e.target.value })} />
            <input className="input" placeholder="entity_id" value={filters.entity_id} onChange={(e) => setFilters({ ...filters, entity_id: e.target.value })} />
            <input className="input" type="date" value={filters.date_from} onChange={(e) => setFilters({ ...filters, date_from: e.target.value })} />
            <input className="input" type="date" value={filters.date_to} onChange={(e) => setFilters({ ...filters, date_to: e.target.value })} />
            <button className="btn" type="button" onClick={() => load(me, filters)}>
              Aplicar filtro
            </button>
          </div>
        </div>

        {selected ? (
          <div className="card">
            <div className="platformSectionHead">
              <div>
                <div className="platformSectionEyebrow">Detalhe</div>
                <h2>
                  {selected.action} · {selected.entity_type}
                </h2>
              </div>
            </div>
            <div className="platformSummaryList">
              <div>Quando: {formatDateTime(selected.created_at)}</div>
              <div>ID entidade: {selected.entity_id}</div>
              <div>Ator: {selected.actor_role || '-'}</div>
            </div>
            <div className="platformGrid">
              <div className="card">
                <div className="platformSectionEyebrow">Antes</div>
                <pre>{JSON.stringify(selected.old_values || {}, null, 2)}</pre>
              </div>
              <div className="card">
                <div className="platformSectionEyebrow">Depois</div>
                <pre>{JSON.stringify(selected.new_values || {}, null, 2)}</pre>
              </div>
            </div>
          </div>
        ) : null}
      </div>

      <div style={{ height: 16 }} />

      <div className="card">
        <table className="table">
          <thead>
            <tr>
              <th>Quando</th>
              <th>Ação</th>
              <th>Entidade</th>
              <th>ID</th>
              <th>Ator</th>
              <th>Ações</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.id}>
                <td>{formatDateTime(item.created_at)}</td>
                <td>{item.action}</td>
                <td>{item.entity_type}</td>
                <td>{item.entity_id}</td>
                <td>{item.actor_role || '-'}</td>
                <td className="platformActionCell">
                  <button className="btn" type="button" onClick={() => setSelected(item)}>
                    Ver detalhe
                  </button>
                </td>
              </tr>
            ))}
            {!items.length ? (
              <tr>
                <td colSpan={6}>Nenhum evento de auditoria encontrado.</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </PlatformShell>
  );
}
