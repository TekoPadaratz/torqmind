'use client';

import { FormEvent, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../../components/PlatformShell';
import { api, apiGet } from '../../lib/api';
import { formatDateOnly } from '../../lib/format';
import { loadSession } from '../../lib/session';

export const dynamic = 'force-dynamic';

function emptyAccess(role = 'tenant_admin') {
  return { role, channel_id: '', id_empresa: '', id_filial: '', is_enabled: true, valid_from: '', valid_until: '' };
}

function emptyUser(role = 'tenant_admin') {
  return {
    nome: '',
    email: '',
    password: '',
    role,
    is_enabled: true,
    valid_from: '',
    valid_until: '',
    must_change_password: true,
    locked_until: '',
    reset_failed_login: false,
    accesses: [emptyAccess(role)],
  };
}

function emptyContact() {
  return {
    user_id: '',
    telegram_chat_id: '',
    telegram_username: '',
    telegram_enabled: false,
    email: '',
    phone: '',
  };
}

function toDateInput(value: any) {
  return value ? String(value).slice(0, 10) : '';
}

function toDatetimeInput(value: any) {
  if (!value) return '';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return '';
  return parsed.toISOString().slice(0, 16);
}

export default function PlatformUsersPage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [items, setItems] = useState<any[]>([]);
  const [companies, setCompanies] = useState<any[]>([]);
  const [channels, setChannels] = useState<any[]>([]);
  const [editingUserId, setEditingUserId] = useState<string | null>(null);
  const [form, setForm] = useState<any>(emptyUser());
  const [contactForm, setContactForm] = useState<any>(emptyContact());
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);

  async function load(session: any) {
    setLoading(true);
    try {
      const tasks: Promise<any>[] = [apiGet('/platform/users?limit=200'), apiGet('/platform/companies?limit=200')];
      if (session?.user_role === 'platform_master') {
        tasks.push(apiGet('/platform/channels?limit=200'));
      }
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

  function resetForms() {
    setEditingUserId(null);
    setForm(emptyUser());
    setContactForm(emptyContact());
  }

  function selectUser(user: any) {
    const firstRole = user?.role || 'tenant_admin';
    setEditingUserId(user.id);
    setForm({
      nome: user.nome || '',
      email: user.email || '',
      password: '',
      role: firstRole,
      is_enabled: Boolean(user.is_enabled),
      valid_from: toDateInput(user.valid_from),
      valid_until: toDateInput(user.valid_until),
      must_change_password: Boolean(user.must_change_password),
      locked_until: toDatetimeInput(user.locked_until),
      reset_failed_login: false,
      accesses: (user.accesses || []).length
        ? user.accesses.map((access: any) => ({
            role: firstRole,
            channel_id: access.channel_id ? String(access.channel_id) : '',
            id_empresa: access.id_empresa ? String(access.id_empresa) : '',
            id_filial: access.id_filial ? String(access.id_filial) : '',
            is_enabled: Boolean(access.is_enabled),
            valid_from: toDateInput(access.valid_from),
            valid_until: toDateInput(access.valid_until),
          }))
        : [emptyAccess(firstRole)],
    });
    setContactForm({
      user_id: user.id,
      telegram_chat_id: user.telegram_chat_id || '',
      telegram_username: user.telegram_username || '',
      telegram_enabled: Boolean(user.telegram_enabled),
      email: user.contact_email || user.email || '',
      phone: user.contact_phone || '',
    });
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

  function setRole(role: string) {
    setForm((current: any) => ({
      ...current,
      role,
      accesses:
        role === 'platform_admin' || role === 'platform_master'
          ? [emptyAccess(role)]
          : current.accesses.map((access: any) => ({ ...access, role })),
    }));
  }

  function updateAccess(index: number, patch: any) {
    const accesses = [...form.accesses];
    accesses[index] = { ...accesses[index], ...patch, role: form.role };
    setForm({ ...form, accesses });
  }

  async function submit(event: FormEvent) {
    event.preventDefault();
    setSaving(true);
    setError('');
    try {
      const payload = {
        ...form,
        valid_from: form.valid_from || null,
        valid_until: form.valid_until || null,
        locked_until: form.locked_until || null,
        accesses:
          form.role === 'platform_admin' || form.role === 'platform_master'
            ? [{ role: form.role, channel_id: null, id_empresa: null, id_filial: null, is_enabled: true, valid_from: null, valid_until: null }]
            : form.accesses.map((access: any) => ({
                ...access,
                role: form.role,
                channel_id: access.channel_id ? Number(access.channel_id) : null,
                id_empresa: access.id_empresa ? Number(access.id_empresa) : null,
                id_filial: access.id_filial ? Number(access.id_filial) : null,
                valid_from: access.valid_from || null,
                valid_until: access.valid_until || null,
              })),
      };
      if (editingUserId) {
        await api.patch(`/platform/users/${editingUserId}`, payload);
      } else {
        await api.post('/platform/users', payload);
      }
      await load(me);
      resetForms();
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao salvar usuário.');
    } finally {
      setSaving(false);
    }
  }

  async function saveContacts(event: FormEvent) {
    event.preventDefault();
    if (!contactForm.user_id) return;
    setSaving(true);
    setError('');
    try {
      await api.put(`/platform/users/${contactForm.user_id}/contacts`, {
        telegram_chat_id: contactForm.telegram_chat_id || null,
        telegram_username: contactForm.telegram_username || null,
        telegram_enabled: contactForm.telegram_enabled,
        email: contactForm.email || null,
        phone: contactForm.phone || null,
      });
      await load(me);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao salvar contato do usuário.');
    } finally {
      setSaving(false);
    }
  }

  return (
    <PlatformShell
      title="Usuários e acessos"
      subtitle="Ciclo completo de cadastro, edição, vigência, lock/unlock, senha inicial e vínculos explícitos por empresa, filial ou canal."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}

      <div className="platformGrid">
        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">{editingUserId ? 'Edição' : 'Novo usuário'}</div>
              <h2>{editingUserId ? 'Atualizar cadastro e vínculo' : 'Cadastro operacional'}</h2>
            </div>
            {editingUserId ? (
              <button className="btn" type="button" onClick={resetForms}>
                Novo usuário
              </button>
            ) : null}
          </div>

          <form className="platformStack" onSubmit={submit}>
            <div className="platformFormGrid">
              <input className="input" placeholder="Nome" value={form.nome} onChange={(e) => setForm({ ...form, nome: e.target.value })} />
              <input className="input" placeholder="Email/login" value={form.email} onChange={(e) => setForm({ ...form, email: e.target.value })} />
              <input
                className="input"
                type="password"
                placeholder={editingUserId ? 'Nova senha opcional' : 'Senha inicial'}
                value={form.password}
                onChange={(e) => setForm({ ...form, password: e.target.value })}
              />
              <select className="input" value={form.role} onChange={(e) => setRole(e.target.value)}>
                <option value="tenant_admin">tenant_admin</option>
                <option value="tenant_manager">tenant_manager</option>
                <option value="tenant_viewer">tenant_viewer</option>
                {me?.user_role === 'platform_master' ? <option value="channel_admin">channel_admin</option> : null}
                {me?.user_role === 'platform_master' ? <option value="platform_admin">platform_admin</option> : null}
              </select>
              <input className="input" type="date" value={form.valid_from} onChange={(e) => setForm({ ...form, valid_from: e.target.value })} />
              <input className="input" type="date" value={form.valid_until} onChange={(e) => setForm({ ...form, valid_until: e.target.value })} />
              <input
                className="input"
                type="datetime-local"
                value={form.locked_until}
                onChange={(e) => setForm({ ...form, locked_until: e.target.value })}
              />
            </div>

            {form.role !== 'platform_admin' && form.role !== 'platform_master'
              ? form.accesses.map((access: any, index: number) => (
                  <div key={`${index}-${form.role}`} className="platformAccessCard">
                    <div className="platformFormGrid">
                      {form.role === 'channel_admin' ? (
                        <select
                          className="input"
                          value={access.channel_id}
                          onChange={(e) => updateAccess(index, { channel_id: e.target.value, id_empresa: '', id_filial: '' })}
                        >
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
                      <label className="platformCheckbox">
                        <input type="checkbox" checked={access.is_enabled} onChange={(e) => updateAccess(index, { is_enabled: e.target.checked })} />
                        Vínculo ativo
                      </label>
                      {form.accesses.length > 1 ? (
                        <button className="btn" type="button" onClick={() => setForm({ ...form, accesses: form.accesses.filter((_: any, i: number) => i !== index) })}>
                          Remover vínculo
                        </button>
                      ) : null}
                    </div>
                  </div>
                ))
              : (
                  <div className="platformFieldHint">Perfis internos usam vínculo global único e continuam fora do escopo do cliente final.</div>
                )}

            <div className="platformInlineFilters">
              {form.role !== 'platform_admin' && form.role !== 'platform_master' ? (
                <button className="btn" type="button" onClick={() => setForm({ ...form, accesses: [...form.accesses, emptyAccess(form.role)] })}>
                  Adicionar vínculo
                </button>
              ) : null}
              <label className="platformCheckbox">
                <input type="checkbox" checked={form.is_enabled} onChange={(e) => setForm({ ...form, is_enabled: e.target.checked })} />
                Usuário habilitado
              </label>
              <label className="platformCheckbox">
                <input type="checkbox" checked={form.must_change_password} onChange={(e) => setForm({ ...form, must_change_password: e.target.checked })} />
                Obrigar troca de senha
              </label>
              <label className="platformCheckbox">
                <input type="checkbox" checked={form.reset_failed_login} onChange={(e) => setForm({ ...form, reset_failed_login: e.target.checked })} />
                Resetar bloqueio/tentativas
              </label>
              <button className="btn" type="submit" disabled={saving}>
                {saving ? 'Salvando...' : editingUserId ? 'Salvar usuário' : 'Criar usuário'}
              </button>
            </div>
          </form>
        </div>

        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Contato</div>
              <h2>Telegram e comunicação</h2>
            </div>
          </div>
          <form className="platformFormGrid" onSubmit={saveContacts}>
            <select className="input" value={contactForm.user_id} onChange={(e) => setContactForm({ ...contactForm, user_id: e.target.value })}>
              <option value="">Selecione um usuário</option>
              {items.map((user) => (
                <option key={user.id} value={user.id}>
                  {user.nome} - {user.email}
                </option>
              ))}
            </select>
            <input className="input" placeholder="telegram_chat_id" value={contactForm.telegram_chat_id} onChange={(e) => setContactForm({ ...contactForm, telegram_chat_id: e.target.value })} />
            <input className="input" placeholder="@username" value={contactForm.telegram_username} onChange={(e) => setContactForm({ ...contactForm, telegram_username: e.target.value })} />
            <input className="input" placeholder="email de contato" value={contactForm.email} onChange={(e) => setContactForm({ ...contactForm, email: e.target.value })} />
            <input className="input" placeholder="telefone" value={contactForm.phone} onChange={(e) => setContactForm({ ...contactForm, phone: e.target.value })} />
            <label className="platformCheckbox">
              <input type="checkbox" checked={contactForm.telegram_enabled} onChange={(e) => setContactForm({ ...contactForm, telegram_enabled: e.target.checked })} />
              Telegram habilitado
            </label>
            <button className="btn" type="submit" disabled={!contactForm.user_id || saving}>
              Salvar contato
            </button>
          </form>
        </div>
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
              <th>Lock</th>
              <th>Vínculos</th>
              <th>Ações</th>
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
                <td>{item.locked_until ? formatDateOnly(item.locked_until) : item.failed_login_count ? `${item.failed_login_count} falhas` : '-'}</td>
                <td className="platformAccessListCell">
                  {(item.accesses || []).map((access: any, index: number) => (
                    <div key={index}>
                      {access.channel_name ? `Canal: ${access.channel_name}` : `Empresa: ${access.tenant_name || access.id_empresa || 'Global'}`}
                      {access.branch_name ? ` / Filial: ${access.branch_name}` : ''}
                    </div>
                  ))}
                </td>
                <td className="platformActionCell">
                  <button className="btn" type="button" onClick={() => selectUser(item)}>
                    Editar
                  </button>
                  <button
                    className="btn"
                    type="button"
                    onClick={() =>
                      setContactForm({
                        user_id: item.id,
                        telegram_chat_id: item.telegram_chat_id || '',
                        telegram_username: item.telegram_username || '',
                        telegram_enabled: Boolean(item.telegram_enabled),
                        email: item.contact_email || item.email || '',
                        phone: item.contact_phone || '',
                      })
                    }
                  >
                    Contato
                  </button>
                </td>
              </tr>
            ))}
            {!items.length && !loading ? (
              <tr>
                <td colSpan={8}>Nenhum usuário encontrado.</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </PlatformShell>
  );
}
