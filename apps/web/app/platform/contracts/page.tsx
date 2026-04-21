'use client';

import { FormEvent, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../../components/PlatformShell';
import { api, apiGet } from '../../lib/api';
import { formatCurrency, formatDateOnly } from '../../lib/format';
import { loadSession } from '../../lib/session';

export const dynamic = 'force-dynamic';

const emptyForm = {
  tenant_id: '',
  channel_id: '',
  plan_name: '',
  monthly_amount: '',
  billing_day: '10',
  issue_day: '5',
  start_date: '',
  end_date: '',
  commission_first_year_pct: '10',
  commission_recurring_pct: '5',
  is_enabled: true,
  notes: '',
};

function toDateInput(value: any) {
  return value ? String(value).slice(0, 10) : '';
}

export default function PlatformContractsPage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [items, setItems] = useState<any[]>([]);
  const [companies, setCompanies] = useState<any[]>([]);
  const [channels, setChannels] = useState<any[]>([]);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [form, setForm] = useState<any>(emptyForm);
  const [error, setError] = useState('');

  async function load(session: any) {
    const [contractsRes, companiesRes, channelsRes] = await Promise.all([
      apiGet('/platform/contracts?limit=200'),
      apiGet('/platform/companies?limit=200'),
      apiGet('/platform/channels?limit=200'),
    ]);
    setItems(contractsRes?.items || []);
    setCompanies(companiesRes?.items || []);
    setChannels(channelsRes?.items || []);
  }

  function resetForm() {
    setEditingId(null);
    setForm(emptyForm);
  }

  function selectContract(item: any) {
    setEditingId(Number(item.id));
    setForm({
      tenant_id: String(item.tenant_id),
      channel_id: item.channel_id ? String(item.channel_id) : '',
      plan_name: item.plan_name || '',
      monthly_amount: String(item.monthly_amount || ''),
      billing_day: String(item.billing_day || 10),
      issue_day: String(item.issue_day || 5),
      start_date: toDateInput(item.start_date),
      end_date: toDateInput(item.end_date),
      commission_first_year_pct: String(item.commission_first_year_pct || ''),
      commission_recurring_pct: String(item.commission_recurring_pct || ''),
      is_enabled: Boolean(item.is_enabled),
      notes: item.notes || '',
    });
  }

  useEffect(() => {
    const boot = async () => {
      const session = await loadSession(router, 'platform');
      if (!session) return;
      setMe(session);
      try {
        await load(session);
      } catch (err: any) {
        setError(err?.message || 'Falha ao carregar contratos.');
      }
    };
    boot();
  }, [router]);

  if (!me) return null;

  async function submit(event: FormEvent) {
    event.preventDefault();
    try {
      const payload = {
        tenant_id: Number(form.tenant_id),
        channel_id: form.channel_id ? Number(form.channel_id) : null,
        plan_name: form.plan_name,
        monthly_amount: Number(form.monthly_amount || 0),
        billing_day: Number(form.billing_day),
        issue_day: Number(form.issue_day),
        start_date: form.start_date,
        end_date: form.end_date || null,
        commission_first_year_pct: Number(form.commission_first_year_pct || 0),
        commission_recurring_pct: Number(form.commission_recurring_pct || 0),
        is_enabled: form.is_enabled,
        notes: form.notes || null,
      };
      if (editingId) {
        await api.patch(`/platform/contracts/${editingId}`, payload);
      } else {
        await api.post('/platform/contracts', payload);
      }
      await load(me);
      resetForm();
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao salvar contrato.');
    }
  }

  return (
    <PlatformShell
      title="Contratos"
      subtitle="Defina plano, cobrança e comissão. Alterações estruturais em contrato ativo passam a preservar histórico e abrir uma nova versão comercial."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}

      <div className="platformGrid">
        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">{editingId ? 'Edição' : 'Novo contrato'}</div>
              <h2>{editingId ? 'Atualizar configuração comercial' : 'Criar contrato ativo'}</h2>
            </div>
            {editingId ? (
              <button className="btn" type="button" onClick={resetForm}>
                Novo contrato
              </button>
            ) : null}
          </div>
          <form className="platformFormGrid" onSubmit={submit}>
            <select className="input" value={form.tenant_id} onChange={(e) => setForm({ ...form, tenant_id: e.target.value })}>
              <option value="">Empresa</option>
              {companies.map((company) => (
                <option key={company.id_empresa} value={company.id_empresa}>
                  {company.id_empresa} - {company.nome}
                </option>
              ))}
            </select>
            <select className="input" value={form.channel_id} onChange={(e) => setForm({ ...form, channel_id: e.target.value })}>
              <option value="">Sem canal</option>
              {channels.map((channel) => (
                <option key={channel.id} value={channel.id}>
                  {channel.name}
                </option>
              ))}
            </select>
            <input className="input" placeholder="Plano" value={form.plan_name} onChange={(e) => setForm({ ...form, plan_name: e.target.value })} />
            <input className="input" placeholder="Mensalidade" value={form.monthly_amount} onChange={(e) => setForm({ ...form, monthly_amount: e.target.value })} />
            <input className="input" placeholder="Dia de emissão" value={form.issue_day} onChange={(e) => setForm({ ...form, issue_day: e.target.value })} />
            <input className="input" placeholder="Dia de vencimento" value={form.billing_day} onChange={(e) => setForm({ ...form, billing_day: e.target.value })} />
            <input className="input" type="date" value={form.start_date} onChange={(e) => setForm({ ...form, start_date: e.target.value })} />
            <input className="input" type="date" value={form.end_date} onChange={(e) => setForm({ ...form, end_date: e.target.value })} />
            <input className="input" placeholder="% 1º ano" value={form.commission_first_year_pct} onChange={(e) => setForm({ ...form, commission_first_year_pct: e.target.value })} />
            <input className="input" placeholder="% recorrente" value={form.commission_recurring_pct} onChange={(e) => setForm({ ...form, commission_recurring_pct: e.target.value })} />
            <input className="input" placeholder="Notas" value={form.notes} onChange={(e) => setForm({ ...form, notes: e.target.value })} />
            <label className="platformCheckbox">
              <input type="checkbox" checked={form.is_enabled} onChange={(e) => setForm({ ...form, is_enabled: e.target.checked })} />
              Contrato habilitado
            </label>
            <button className="btn" type="submit">
              {editingId ? 'Salvar contrato' : 'Criar contrato'}
            </button>
          </form>
          {editingId ? <div className="platformFieldHint">Se houver mudança estrutural, a API fecha o contrato atual e cria uma nova versão histórica.</div> : null}
        </div>

        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Resumo</div>
              <h2>Carteira contratual</h2>
            </div>
          </div>
          <div className="platformSummaryList">
            <div>Contratos ativos: {items.filter((item) => item.is_enabled).length}</div>
            <div>Empresas contratadas: {new Set(items.map((item) => item.tenant_id)).size}</div>
            <div>Mensalidade ativa: {formatCurrency(items.filter((item) => item.is_enabled).reduce((sum, item) => sum + Number(item.monthly_amount || 0), 0))}</div>
          </div>
        </div>
      </div>

      <div style={{ height: 16 }} />

      <div className="card">
        <table className="table">
          <thead>
            <tr>
              <th>Empresa</th>
              <th>Plano</th>
              <th>Mensalidade</th>
              <th>Canal</th>
              <th>Vigência</th>
              <th>Status</th>
              <th>Ações</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.id}>
                <td>{item.tenant_name}</td>
                <td>{item.plan_name}</td>
                <td>{formatCurrency(item.monthly_amount)}</td>
                <td>{item.channel_name || '-'}</td>
                <td>
                  {formatDateOnly(item.start_date)} até {formatDateOnly(item.end_date)}
                </td>
                <td>{item.is_enabled ? 'ativo' : 'encerrado'}</td>
                <td className="platformActionCell">
                  <button className="btn" type="button" onClick={() => selectContract(item)}>
                    Editar
                  </button>
                </td>
              </tr>
            ))}
            {!items.length ? (
              <tr>
                <td colSpan={7}>Nenhum contrato cadastrado.</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </PlatformShell>
  );
}
