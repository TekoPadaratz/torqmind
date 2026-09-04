'use client';

import Link from 'next/link';
import { FormEvent, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../../components/PlatformShell';
import GridSearchInput from '../../components/ui/GridSearchInput';
import { apiGet, apiPost } from '../../lib/api';
import { formatCurrency, formatDateOnly } from '../../lib/format';
import { loadSession } from '../../lib/session';
import { useGridSearch } from '../../lib/use-grid-search';

export const dynamic = 'force-dynamic';

const emptyForm = {
  nome: '',
  cnpj: '',
  is_enabled: true,
  valid_from: '',
  valid_until: '',
  channel_id: '',
};

function CopyReadonlyField({ label, value, hint }: { label: string; value: string; hint?: string }) {
  const [copied, setCopied] = useState(false);
  if (!value) return null;
  return (
    <label style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
      <span className="platformFieldHint" style={{ fontWeight: 600, color: 'var(--text-primary, inherit)' }}>{label}</span>
      <div style={{ display: 'flex', gap: 8, alignItems: 'stretch' }}>
        <input className="input" value={value} readOnly aria-readonly />
        <button
          type="button"
          className="btn"
          onClick={async () => {
            try {
              await navigator.clipboard.writeText(value);
              setCopied(true);
              window.setTimeout(() => setCopied(false), 2000);
            } catch {
              /* fallback silencioso */
            }
          }}
        >
          {copied ? 'Copiado' : 'Copiar'}
        </button>
      </div>
      {hint ? <span className="platformFieldHint">{hint}</span> : null}
    </label>
  );
}

export default function PlatformCompaniesPage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [items, setItems] = useState<any[]>([]);
  const [channels, setChannels] = useState<any[]>([]);
  const [search, setSearch] = useState('');
  const [status, setStatus] = useState('');
  const [form, setForm] = useState<any>(emptyForm);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [createdCompany, setCreatedCompany] = useState<any>(null);
  const companiesSearch = useGridSearch(items);

  const load = async (session: any, currentSearch = search, currentStatus = status) => {
    setLoading(true);
    try {
      const tasks: Promise<any>[] = [
        apiGet(`/platform/companies?limit=100&search=${encodeURIComponent(currentSearch)}${currentStatus ? `&status=${currentStatus}` : ''}`),
      ];
      if (session?.access?.platform_finance) {
        tasks.push(apiGet('/platform/channels?limit=200'));
      }
      const [companiesRes, channelsRes] = await Promise.all(tasks);
      setItems(companiesRes?.items || []);
      setChannels(channelsRes?.items || []);
    } catch (err: any) {
      setError(err?.message || 'Falha ao carregar empresas.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const boot = async () => {
      const session = await loadSession(router, 'platform');
      if (!session) return;
      setMe(session);
      await load(session, '', '');
    };
    boot();
  }, [router]);

  if (!me) return null;

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSaving(true);
    setError('');
    setCreatedCompany(null);
    try {
      const created = await apiPost('/platform/companies', {
        nome: form.nome,
        cnpj: form.cnpj || null,
        is_enabled: form.is_enabled,
        valid_from: form.valid_from || null,
        valid_until: form.valid_until || null,
        channel_id: form.channel_id ? Number(form.channel_id) : null,
      });
      setCreatedCompany(created);
      setForm(emptyForm);
      await load(me, search, status);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao salvar empresa.');
    } finally {
      setSaving(false);
    }
  }

  return (
    <PlatformShell
      title="Empresas"
      subtitle="Cadastro e acompanhamento de tenants com separação clara entre operação da plataforma e produto do cliente."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}

      <div className="card">
        <div className="platformSectionHead">
          <div>
            <div className="platformSectionEyebrow">Cadastro</div>
            <h2>Nova empresa</h2>
          </div>
        </div>

        <form className="platformFormGrid" onSubmit={handleSubmit}>
          <label style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <span className="platformFieldHint" style={{ fontWeight: 600, color: 'var(--text-primary, inherit)' }}>Nome da empresa</span>
            <input className="input" placeholder="Nome da empresa" value={form.nome} onChange={(e) => setForm({ ...form, nome: e.target.value })} required />
          </label>
          <label style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <span className="platformFieldHint" style={{ fontWeight: 600, color: 'var(--text-primary, inherit)' }}>CNPJ</span>
            <input className="input" placeholder="CNPJ" value={form.cnpj} onChange={(e) => setForm({ ...form, cnpj: e.target.value })} />
          </label>
          <label style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <span className="platformFieldHint" style={{ fontWeight: 600, color: 'var(--text-primary, inherit)' }}>Vigência — início</span>
            <input className="input" type="date" value={form.valid_from} onChange={(e) => setForm({ ...form, valid_from: e.target.value })} />
          </label>
          <label style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <span className="platformFieldHint" style={{ fontWeight: 600, color: 'var(--text-primary, inherit)' }}>Vigência — fim</span>
            <input className="input" type="date" value={form.valid_until} onChange={(e) => setForm({ ...form, valid_until: e.target.value })} />
          </label>
          {me?.access?.platform_finance ? (
            <select className="input" value={form.channel_id} onChange={(e) => setForm({ ...form, channel_id: e.target.value })}>
              <option value="">Sem canal</option>
              {channels.map((channel) => (
                <option key={channel.id} value={channel.id}>
                  {channel.name}
                </option>
              ))}
            </select>
          ) : (
            <div className="platformFieldHint">Canal é definido pelo Master.</div>
          )}
          <label className="platformCheckbox">
            <input type="checkbox" checked={form.is_enabled} onChange={(e) => setForm({ ...form, is_enabled: e.target.checked })} />
            Habilitada
          </label>
          <button className="btn" type="submit" disabled={saving}>
            {saving ? 'Salvando...' : 'Criar empresa'}
          </button>
        </form>

        {createdCompany?.ingest_key ? (
          <div style={{ marginTop: 16 }}>
            <CopyReadonlyField
              label="Ingest Key (Agent)"
              value={String(createdCompany.ingest_key)}
              hint="Uma chave por empresa. Copie e configure no posto (config.enc → api.ingest_key). Não é editável."
            />
            {createdCompany.id_empresa ? (
              <div className="platformFieldHint" style={{ marginTop: 8 }}>
                Empresa criada com ID {createdCompany.id_empresa}.{' '}
                <Link href={`/platform/companies/${createdCompany.id_empresa}`}>Abrir cadastro</Link>
              </div>
            ) : null}
          </div>
        ) : null}
      </div>

      <div style={{ height: 16 }} />

      <div className="card">
        <div className="platformSectionHead">
          <div>
            <div className="platformSectionEyebrow">Carteira</div>
            <h2>Lista operacional</h2>
          </div>
          <div className="platformInlineFilters">
            <input className="input" placeholder="Buscar por nome ou CNPJ" value={search} onChange={(e) => setSearch(e.target.value)} />
            <select className="input" value={status} onChange={(e) => setStatus(e.target.value)}>
              <option value="">Todos os status</option>
              <option value="active">active</option>
              <option value="trial">trial</option>
              <option value="overdue">overdue</option>
              <option value="grace">grace</option>
              <option value="suspended_readonly">suspended_readonly</option>
              <option value="suspended_total">suspended_total</option>
              <option value="cancelled">cancelled</option>
            </select>
            <button className="btn" onClick={() => load(me, search, status)}>
              Filtrar
            </button>
          </div>
        </div>
        <GridSearchInput value={companiesSearch.query} onChange={companiesSearch.setQuery} aria-label="Pesquisar empresas carregadas" />

        <table className="table">
          <thead>
            <tr>
              <th>ID</th>
              <th>Empresa</th>
              <th>Status</th>
              <th>Canal</th>
              <th>Valor mensal</th>
              <th>Vigência</th>
            </tr>
          </thead>
          <tbody>
            {companiesSearch.filteredRows.map((item) => (
              <tr key={item.id_empresa}>
                <td>{item.id_empresa}</td>
                <td>
                  <Link href={`/platform/companies/${item.id_empresa}`}>{item.nome}</Link>
                </td>
                <td>{item.status}</td>
                <td>{item.channel_name || '-'}</td>
                <td>{item.monthly_amount != null ? formatCurrency(item.monthly_amount) : '-'}</td>
                <td>{formatDateOnly(item.valid_until || item.valid_from)}</td>
              </tr>
            ))}
            {!companiesSearch.filteredRows.length && !loading ? (
              <tr>
                <td colSpan={6}>Nenhuma empresa encontrada.</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </PlatformShell>
  );
}
