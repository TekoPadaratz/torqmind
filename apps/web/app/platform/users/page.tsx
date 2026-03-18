'use client';

import { FormEvent, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../../components/PlatformShell';
import { apiGet, apiPost } from '../../lib/api';
import { formatDateOnly } from '../../lib/format';
import { loadSession } from '../../lib/session';

export const dynamic = 'force-dynamic';

const baseAccess = { role: 'tenant_admin', channel_id: '', id_empresa: '', id_filial: '', is_enabled: true, valid_from: '', valid_until: '' };
const baseUser = {
  nome: '',
  email: '',
  password: '',
  role: 'tenant_admin',
  is_enabled: true,
  valid_from: '',
  valid_until: '',
  must_change_password: true,
  accesses: [baseAccess],
};

export default function PlatformUsersPage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [items, setItems] = useState<any[]>([]);
  const [companies, setCompanies] = useState<any[]>([]);
  const [channels, setChannels] = useState<any[]>([]);
  const [form, setForm] = useState<any>(baseUser);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);

  async function load(session: any) {
    setLoading(true);
    try {
      const tasks: Promise<any>[] = [apiGet('/platform/users?limit=200'), apiGet('/platform/companies?limit=200')];
      if (session?.access?.platform_finance) tasks.push(apiGet('/platform/channels?limit=200'));
      const [usersRes, companiesRes, channelsRes] = await Promise.all(tasks);
      setItems(usersRes?.items || []);
      setCompanies(companiesRes?.items || []);
      setChannels(channelsRes?.items || []);
      setError('');
    } catch (err: any) {
      setError(err?.message || 'Falha ao carregar usuários.');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    const boot = async () => {
      const session = await loadSession(router, 'platform');
      if (!session) return;
      setMe(session);
      await load(session);
    };
    boot();
  }, [router]);

  if (!me) return null;

  function updateAccess(index: number, patch: any) {
    const accesses = [...form.accesses];
    accesses[index] = { ...accesses[index], ...patch, role: form.role };
    setForm({ ...form, accesses });
  }

  async function submit(event: FormEvent) {
    event.preventDefault();
    setSaving(true);
    try {
      await apiPost('/platform/users', {
        ...form,
        valid_from: form.valid_from || null,
        valid_until: form.valid_until || null,
        accesses: form.accesses.map((access: any) => ({
          ...access,
          role: form.role,
          channel_id: access.channel_id ? Number(access.channel_id) : null,
          id_empresa: access.id_empresa ? Number(access.id_empresa) : null,
          id_filial: access.id_filial ? Number(access.id_filial) : null,
          valid_from: access.valid_from || null,
          valid_until: access.valid_until || null,
        })),
      });
      setForm(baseUser);
      await load(me);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao salvar usuário.');
    } finally {
      setSaving(false);
    }
  }

  return (
    <PlatformShell
      title="Usuários e acessos"
      subtitle="Gestão explícita de papéis, vigência e vínculo por empresa, filial ou canal, sem confiar apenas em menu oculto."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}

      <div className="card">
        <div className="platformSectionHead">
          <div>
            <div className="platformSectionEyebrow">Novo usuário</div>
            <h2>Cadastro e vínculo</h2>
          </div>
        </div>

        <form className="platformStack" onSubmit={submit}>
          <div className="platformFormGrid">
            <input className="input" placeholder="Nome" value={form.nome} onChange={(e) => setForm({ ...form, nome: e.target.value })} />
            <input className="input" placeholder="Email/login" value={form.email} onChange={(e) => setForm({ ...form, email: e.target.value })} />
            <input className="input" type="password" placeholder="Senha inicial" value={form.password} onChange={(e) => setForm({ ...form, password: e.target.value })} />
            <select className="input" value={form.role} onChange={(e) => setForm({ ...form, role: e.target.value, accesses: form.accesses.map((a: any) => ({ ...a, role: e.target.value })) })}>
              <option value="tenant_admin">tenant_admin</option>
              <option value="tenant_manager">tenant_manager</option>
              <option value="tenant_viewer">tenant_viewer</option>
              {me?.access?.platform_operations ? <option value="channel_admin">channel_admin</option> : null}
              {me?.user_role === 'platform_master' ? <option value="platform_admin">platform_admin</option> : null}
            </select>
            <input className="input" type="date" value={form.valid_from} onChange={(e) => setForm({ ...form, valid_from: e.target.value })} />
            <input className="input" type="date" value={form.valid_until} onChange={(e) => setForm({ ...form, valid_until: e.target.value })} />
          </div>

          {form.accesses.map((access: any, index: number) => (
            <div key={index} className="platformAccessCard">
              <div className="platformFormGrid">
                {form.role === 'channel_admin' ? (
                  <select className="input" value={access.channel_id} onChange={(e) => updateAccess(index, { channel_id: e.target.value, id_empresa: '', id_filial: '' })}>
                    <option value="">Canal</option>
                    {channels.map((channel) => (
                      <option key={channel.id} value={channel.id}>
                        {channel.name}
                      </option>
                    ))}
                  </select>
                ) : (
                  <>
                    <select className="input" value={access.id_empresa} onChange={(e) => updateAccess(index, { id_empresa: e.target.value })}>
                      <option value="">Empresa</option>
                      {companies.map((company) => (
                        <option key={company.id_empresa} value={company.id_empresa}>
                          {company.id_empresa} - {company.nome}
                        </option>
                      ))}
                    </select>
                    <input className="input" placeholder="Filial opcional" value={access.id_filial} onChange={(e) => updateAccess(index, { id_filial: e.target.value })} />
                  </>
                )}
                <input className="input" type="date" value={access.valid_from} onChange={(e) => updateAccess(index, { valid_from: e.target.value })} />
                <input className="input" type="date" value={access.valid_until} onChange={(e) => updateAccess(index, { valid_until: e.target.value })} />
              </div>
            </div>
          ))}

          <div className="platformInlineFilters">
            {form.role !== 'platform_admin' && form.role !== 'platform_master' ? (
              <button
                className="btn"
                type="button"
                onClick={() => setForm({ ...form, accesses: [...form.accesses, { ...baseAccess, role: form.role }] })}
              >
                Adicionar vínculo
              </button>
            ) : null}
            <label className="platformCheckbox">
              <input type="checkbox" checked={form.must_change_password} onChange={(e) => setForm({ ...form, must_change_password: e.target.checked })} />
              Obrigar troca de senha
            </label>
            <button className="btn" type="submit" disabled={saving}>
              {saving ? 'Salvando...' : 'Criar usuário'}
            </button>
          </div>
        </form>
      </div>

      <div style={{ height: 16 }} />

      <div className="card">
        <div className="platformSectionHead">
          <div>
            <div className="platformSectionEyebrow">Operação</div>
            <h2>Usuários cadastrados</h2>
          </div>
        </div>
        <table className="table">
          <thead>
            <tr>
              <th>Nome</th>
              <th>Email</th>
              <th>Papel</th>
              <th>Vigência</th>
              <th>Último acesso</th>
              <th>Vínculos</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.id}>
                <td>{item.nome}</td>
                <td>{item.email}</td>
                <td>{item.role}</td>
                <td>{formatDateOnly(item.valid_until || item.valid_from)}</td>
                <td>{formatDateOnly(item.last_login_at)}</td>
                <td className="platformAccessListCell">
                  {(item.accesses || []).map((access: any, index: number) => (
                    <div key={index}>
                      {access.channel_name ? `Canal: ${access.channel_name}` : `Empresa: ${access.tenant_name || access.id_empresa}`}
                      {access.branch_name ? ` / Filial: ${access.branch_name}` : ''}
                    </div>
                  ))}
                </td>
              </tr>
            ))}
            {!items.length && !loading ? (
              <tr>
                <td colSpan={6}>Nenhum usuário encontrado.</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </PlatformShell>
  );
}
