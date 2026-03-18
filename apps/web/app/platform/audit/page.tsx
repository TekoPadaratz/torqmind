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
  const [error, setError] = useState('');

  useEffect(() => {
    const boot = async () => {
      const session = await loadSession(router, 'platform');
      if (!session) return;
      setMe(session);
      try {
        const res = await apiGet('/platform/audit?limit=200');
        setItems(res?.items || []);
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
      subtitle="Trilha central dos eventos administrativos, comerciais e financeiros executados dentro do backoffice da plataforma."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}

      <div className="card">
        <table className="table">
          <thead>
            <tr>
              <th>Quando</th>
              <th>Ação</th>
              <th>Entidade</th>
              <th>ID</th>
              <th>Ator</th>
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
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </PlatformShell>
  );
}
