'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../../components/PlatformShell';
import { api } from '../../lib/api';
import { formatCurrency, formatDateOnly, formatDateTime } from '../../lib/format';
import { loadSession } from '../../lib/session';

export const dynamic = 'force-dynamic';

export default function PlatformChannelPayablesPage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [items, setItems] = useState<any[]>([]);
  const [error, setError] = useState('');

  async function load(session: any) {
    const res = await api.get('/platform/channel-payables?limit=200');
    setItems(res.data?.items || []);
  }

  useEffect(() => {
    const boot = async () => {
      const session = await loadSession(router, 'platform');
      if (!session) return;
      setMe(session);
      try {
        await load(session);
      } catch (err: any) {
        setError(err?.message || 'Falha ao carregar payables.');
      }
    };
    boot();
  }, [router]);

  if (!me) return null;

  async function markPaid(id: number) {
    try {
      await api.post(`/platform/channel-payables/${id}/pay`, {});
      await load(me);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao marcar payable como pago.');
    }
  }

  async function cancel(id: number) {
    try {
      await api.post(`/platform/channel-payables/${id}/cancel`, { notes: 'Cancelado manualmente' });
      await load(me);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao cancelar payable.');
    }
  }

  return (
    <PlatformShell
      title="Contas a pagar de canal"
      subtitle="Repasse gerado somente após baixa do recebível, respeitando comissão de primeiro ano e recorrência."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}

      <div className="card">
        <table className="table">
          <thead>
            <tr>
              <th>Canal</th>
              <th>Empresa</th>
              <th>Competência</th>
              <th>Base</th>
              <th>%</th>
              <th>Valor a pagar</th>
              <th>Status</th>
              <th>Pago em</th>
              <th>Ações</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.id}>
                <td>{item.channel_name}</td>
                <td>{item.tenant_name}</td>
                <td>{formatDateOnly(item.competence_month)}</td>
                <td>{formatCurrency(item.gross_amount)}</td>
                <td>{item.commission_pct}%</td>
                <td>{formatCurrency(item.payable_amount)}</td>
                <td>{item.status}</td>
                <td>{item.paid_at ? formatDateTime(item.paid_at) : '-'}</td>
                <td className="platformActionCell">
                  <button className="btn" onClick={() => markPaid(item.id)}>
                    Marcar pago
                  </button>
                  <button className="btn" onClick={() => cancel(item.id)}>
                    Cancelar
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </PlatformShell>
  );
}
