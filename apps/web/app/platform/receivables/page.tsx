'use client';

import { FormEvent, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../../components/PlatformShell';
import { api, apiGet } from '../../lib/api';
import { formatCurrency, formatDateOnly, formatDateTime } from '../../lib/format';
import { loadSession } from '../../lib/session';

export const dynamic = 'force-dynamic';

export default function PlatformReceivablesPage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [items, setItems] = useState<any[]>([]);
  const [companies, setCompanies] = useState<any[]>([]);
  const [generation, setGeneration] = useState({ competence_month: '', as_of: '', months_ahead: 0, tenant_id: '' });
  const [error, setError] = useState('');

  async function load(session: any) {
    const [receivablesRes, companiesRes] = await Promise.all([
      apiGet('/platform/receivables?limit=200'),
      apiGet('/platform/companies?limit=200'),
    ]);
    setItems(receivablesRes?.items || []);
    setCompanies(companiesRes?.items || []);
  }

  useEffect(() => {
    const boot = async () => {
      const session = await loadSession(router, 'platform');
      if (!session) return;
      setMe(session);
      try {
        await load(session);
      } catch (err: any) {
        setError(err?.message || 'Falha ao carregar contas a receber.');
      }
    };
    boot();
  }, [router]);

  if (!me) return null;

  async function generate(event: FormEvent) {
    event.preventDefault();
    try {
      await api.post('/platform/receivables/generate', {
        competence_month: generation.competence_month || null,
        as_of: generation.as_of || null,
        months_ahead: Number(generation.months_ahead || 0),
        tenant_id: generation.tenant_id ? Number(generation.tenant_id) : null,
      });
      await load(me);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao gerar contas.');
    }
  }

  async function action(id: number, kind: 'emit' | 'pay' | 'cancel') {
    try {
      if (kind === 'emit') {
        await api.post(`/platform/receivables/${id}/emit`, {});
      }
      if (kind === 'pay') {
        const receivedAmount = window.prompt('Valor recebido', '');
        const paymentMethod = window.prompt('Método de pagamento', 'manual');
        await api.post(`/platform/receivables/${id}/pay`, {
          received_amount: receivedAmount ? Number(receivedAmount) : null,
          payment_method: paymentMethod || null,
        });
      }
      if (kind === 'cancel') {
        const notes = window.prompt('Motivo do cancelamento', '');
        await api.post(`/platform/receivables/${id}/cancel`, { notes: notes || null });
      }
      await load(me);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha na ação da cobrança.');
    }
  }

  return (
    <PlatformShell
      title="Contas a receber"
      subtitle="Operação manual de emissão e baixa com geração idempotente por competência e repasse automático para canal no pagamento."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}

      <div className="card">
        <div className="platformSectionHead">
          <div>
            <div className="platformSectionEyebrow">Geração</div>
            <h2>Gerar novas competências</h2>
          </div>
        </div>
        <form className="platformFormGrid" onSubmit={generate}>
          <select className="input" value={generation.tenant_id} onChange={(e) => setGeneration({ ...generation, tenant_id: e.target.value })}>
            <option value="">Todas as empresas</option>
            {companies.map((company) => (
              <option key={company.id_empresa} value={company.id_empresa}>
                {company.nome}
              </option>
            ))}
          </select>
          <input className="input" type="date" value={generation.competence_month} onChange={(e) => setGeneration({ ...generation, competence_month: e.target.value })} />
          <input className="input" type="date" value={generation.as_of} onChange={(e) => setGeneration({ ...generation, as_of: e.target.value })} />
          <input className="input" type="number" min={0} max={6} value={generation.months_ahead} onChange={(e) => setGeneration({ ...generation, months_ahead: Number(e.target.value) })} />
          <button className="btn" type="submit">
            Gerar cobranças
          </button>
        </form>
      </div>

      <div style={{ height: 16 }} />

      <div className="card">
        <table className="table">
          <thead>
            <tr>
              <th>Competência</th>
              <th>Empresa</th>
              <th>Valor</th>
              <th>Vencimento</th>
              <th>Status</th>
              <th>Emitido</th>
              <th>Pago</th>
              <th>Canal</th>
              <th>Ações</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.id}>
                <td>{formatDateOnly(item.competence_month)}</td>
                <td>{item.tenant_name}</td>
                <td>{formatCurrency(item.amount)}</td>
                <td>{formatDateOnly(item.due_date)}</td>
                <td>{item.status}</td>
                <td>{item.is_emitted ? formatDateTime(item.emitted_at) : '-'}</td>
                <td>{item.paid_at ? formatDateTime(item.paid_at) : '-'}</td>
                <td>{item.channel_name || '-'}</td>
                <td className="platformActionCell">
                  <button className="btn" onClick={() => action(item.id, 'emit')}>
                    Emitir
                  </button>
                  <button className="btn" onClick={() => action(item.id, 'pay')}>
                    Pagar
                  </button>
                  <button className="btn" onClick={() => action(item.id, 'cancel')}>
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
