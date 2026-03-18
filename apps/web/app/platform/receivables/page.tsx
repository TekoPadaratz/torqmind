'use client';

import { FormEvent, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../../components/PlatformShell';
import { api, apiGet } from '../../lib/api';
import { formatCurrency, formatDateOnly, formatDateTime } from '../../lib/format';
import { loadSession } from '../../lib/session';

export const dynamic = 'force-dynamic';

function toDatetimeInput(value: any) {
  if (!value) return '';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return '';
  return parsed.toISOString().slice(0, 16);
}

export default function PlatformReceivablesPage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [items, setItems] = useState<any[]>([]);
  const [companies, setCompanies] = useState<any[]>([]);
  const [filters, setFilters] = useState({ tenant_id: '', status: '' });
  const [generation, setGeneration] = useState({ competence_month: '', as_of: '', months_ahead: 0, tenant_id: '' });
  const [selected, setSelected] = useState<any>(null);
  const [emitForm, setEmitForm] = useState({ emitted_at: '', notes: '' });
  const [payForm, setPayForm] = useState({ paid_at: '', received_amount: '', payment_method: 'manual', notes: '' });
  const [noteForm, setNoteForm] = useState({ notes: '' });
  const [error, setError] = useState('');

  async function load(session: any, currentFilters = filters) {
    const query = new URLSearchParams({ limit: '200' });
    if (currentFilters.tenant_id) query.set('tenant_id', currentFilters.tenant_id);
    if (currentFilters.status) query.set('status', currentFilters.status);
    const [receivablesRes, companiesRes] = await Promise.all([
      apiGet(`/platform/receivables?${query.toString()}`),
      apiGet('/platform/companies?limit=200'),
    ]);
    setItems(receivablesRes?.items || []);
    setCompanies(companiesRes?.items || []);
    if (selected) {
      const fresh = (receivablesRes?.items || []).find((item: any) => item.id === selected.id);
      setSelected(fresh || null);
    }
  }

  useEffect(() => {
    const boot = async () => {
      const session = await loadSession(router, 'platform');
      if (!session) return;
      setMe(session);
      try {
        await load(session, { tenant_id: '', status: '' });
      } catch (err: any) {
        setError(err?.message || 'Falha ao carregar contas a receber.');
      }
    };
    boot();
  }, [router]);

  if (!me) return null;

  function selectReceivable(item: any) {
    setSelected(item);
    setEmitForm({ emitted_at: toDatetimeInput(item.emitted_at), notes: item.notes || '' });
    setPayForm({
      paid_at: toDatetimeInput(item.paid_at),
      received_amount: item.received_amount ? String(item.received_amount) : String(item.amount || ''),
      payment_method: item.payment_method || 'manual',
      notes: item.notes || '',
    });
    setNoteForm({ notes: item.notes || '' });
  }

  async function generate(event: FormEvent) {
    event.preventDefault();
    try {
      await api.post('/platform/receivables/generate', {
        competence_month: generation.competence_month || null,
        as_of: generation.as_of || null,
        months_ahead: Number(generation.months_ahead || 0),
        tenant_id: generation.tenant_id ? Number(generation.tenant_id) : null,
      });
      await load(me, filters);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao gerar contas.');
    }
  }

  async function runAction(kind: 'emit' | 'unemit' | 'pay' | 'undo-payment' | 'cancel' | 'reopen') {
    if (!selected) return;
    try {
      if (kind === 'emit') {
        await api.post(`/platform/receivables/${selected.id}/emit`, {
          emitted_at: emitForm.emitted_at || null,
          notes: emitForm.notes || null,
        });
      }
      if (kind === 'unemit') {
        await api.post(`/platform/receivables/${selected.id}/unemit`, { notes: noteForm.notes || null });
      }
      if (kind === 'pay') {
        await api.post(`/platform/receivables/${selected.id}/pay`, {
          paid_at: payForm.paid_at || null,
          received_amount: payForm.received_amount ? Number(payForm.received_amount) : null,
          payment_method: payForm.payment_method || null,
          notes: payForm.notes || null,
        });
      }
      if (kind === 'undo-payment') {
        await api.post(`/platform/receivables/${selected.id}/undo-payment`, { notes: noteForm.notes || null });
      }
      if (kind === 'cancel') {
        await api.post(`/platform/receivables/${selected.id}/cancel`, { notes: noteForm.notes || null });
      }
      if (kind === 'reopen') {
        await api.post(`/platform/receivables/${selected.id}/reopen`, { notes: noteForm.notes || null });
      }
      await load(me, filters);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha na ação da cobrança.');
    }
  }

  return (
    <PlatformShell
      title="Contas a receber"
      subtitle="Geração idempotente por competência, emissão manual, baixa manual, reversão segura e repasse automático ao canal sobre o valor efetivamente recebido."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}

      <div className="platformGrid">
        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Geração</div>
              <h2>Competências mensais</h2>
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

        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Filtro</div>
              <h2>Fila operacional</h2>
            </div>
          </div>
          <div className="platformFormGrid">
            <select className="input" value={filters.tenant_id} onChange={(e) => setFilters({ ...filters, tenant_id: e.target.value })}>
              <option value="">Todas as empresas</option>
              {companies.map((company) => (
                <option key={company.id_empresa} value={company.id_empresa}>
                  {company.nome}
                </option>
              ))}
            </select>
            <select className="input" value={filters.status} onChange={(e) => setFilters({ ...filters, status: e.target.value })}>
              <option value="">Todos os status</option>
              <option value="planned">planned</option>
              <option value="open">open</option>
              <option value="issued">issued</option>
              <option value="overdue">overdue</option>
              <option value="paid">paid</option>
              <option value="cancelled">cancelled</option>
            </select>
            <button className="btn" type="button" onClick={() => load(me, filters)}>
              Aplicar filtros
            </button>
          </div>
        </div>
      </div>

      {selected ? (
        <>
          <div style={{ height: 16 }} />
          <div className="platformGrid">
            <div className="card">
              <div className="platformSectionHead">
                <div>
                  <div className="platformSectionEyebrow">Recebível selecionado</div>
                  <h2>
                    {selected.tenant_name} · {formatDateOnly(selected.competence_month)}
                  </h2>
                </div>
              </div>
              <div className="platformSummaryList">
                <div>Valor nominal: {formatCurrency(selected.amount)}</div>
                <div>Status: {selected.status}</div>
                <div>Emitido: {selected.is_emitted ? formatDateTime(selected.emitted_at) : '-'}</div>
                <div>Pago: {selected.paid_at ? formatDateTime(selected.paid_at) : '-'}</div>
                <div>Canal: {selected.channel_name || '-'}</div>
              </div>
            </div>

            <div className="card">
              <div className="platformSectionHead">
                <div>
                  <div className="platformSectionEyebrow">Transições</div>
                  <h2>Operar ciclo financeiro</h2>
                </div>
              </div>
              <div className="platformStack">
                <div className="platformFormGrid">
                  <input className="input" type="datetime-local" value={emitForm.emitted_at} onChange={(e) => setEmitForm({ ...emitForm, emitted_at: e.target.value })} />
                  <input className="input" placeholder="Notas de emissão" value={emitForm.notes} onChange={(e) => setEmitForm({ ...emitForm, notes: e.target.value })} />
                  <button className="btn" type="button" onClick={() => runAction('emit')}>
                    Marcar emitido
                  </button>
                  <button className="btn" type="button" onClick={() => runAction('unemit')}>
                    Desfazer emitido
                  </button>
                </div>
                <div className="platformFormGrid">
                  <input className="input" type="datetime-local" value={payForm.paid_at} onChange={(e) => setPayForm({ ...payForm, paid_at: e.target.value })} />
                  <input className="input" placeholder="Valor recebido" value={payForm.received_amount} onChange={(e) => setPayForm({ ...payForm, received_amount: e.target.value })} />
                  <input className="input" placeholder="Método de pagamento" value={payForm.payment_method} onChange={(e) => setPayForm({ ...payForm, payment_method: e.target.value })} />
                  <input className="input" placeholder="Notas do pagamento" value={payForm.notes} onChange={(e) => setPayForm({ ...payForm, notes: e.target.value })} />
                  <button className="btn" type="button" onClick={() => runAction('pay')}>
                    Marcar pago
                  </button>
                  <button className="btn" type="button" onClick={() => runAction('undo-payment')}>
                    Desfazer pagamento
                  </button>
                </div>
                <div className="platformFormGrid">
                  <input className="input" placeholder="Notas operacionais" value={noteForm.notes} onChange={(e) => setNoteForm({ notes: e.target.value })} />
                  <button className="btn" type="button" onClick={() => runAction('cancel')}>
                    Cancelar
                  </button>
                  <button className="btn" type="button" onClick={() => runAction('reopen')}>
                    Reabrir
                  </button>
                </div>
              </div>
            </div>
          </div>
        </>
      ) : null}

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
                  <button className="btn" type="button" onClick={() => selectReceivable(item)}>
                    Operar
                  </button>
                </td>
              </tr>
            ))}
            {!items.length ? (
              <tr>
                <td colSpan={9}>Nenhuma conta a receber encontrada.</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </PlatformShell>
  );
}
