'use client';

import { FormEvent, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../../components/PlatformShell';
import { api, apiGet } from '../../lib/api';
import { formatDateOnly } from '../../lib/format';
import { loadSession } from '../../lib/session';
import {
  FALLBACK_SCREEN_TREE,
  TV_SCREEN_OPTIONS,
  allProductPermissionKeys,
  toggleMenuPermission,
  togglePanelPermission,
  type ScreenMenu,
} from '../../lib/screen-permissions';
import { USERNAME_ERROR_MESSAGE, normalizeUsernameInput, validateUsernameInput } from '../../lib/username-policy.mjs';

export const dynamic = 'force-dynamic';

function emptyAccess(role = 'tenant_admin') {
  return { role, channel_id: '', id_empresa: '', id_filial: '', is_enabled: true, valid_from: '', valid_until: '' };
}

function emptyUser(role = 'tenant_admin') {
  return {
    nome: '',
    email: '',
    username: '',
    password: '',
    role,
    is_enabled: true,
    valid_from: '',
    valid_until: '',
    must_change_password: true,
    locked_until: '',
    reset_failed_login: false,
    screen_permissions: [] as string[],
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

function toDateInput(value?: string | null) {
  if (!value) return '';
  return String(value).slice(0, 10);
}

function toDatetimeInput(value?: string | null) {
  if (!value) return '';
  return String(value).slice(0, 16);
}

const ROLE_OPTIONS = [
  { value: 'platform_master', label: 'Platform Master' },
  { value: 'platform_admin', label: 'Platform Admin' },
  { value: 'product_global', label: 'Product Global' },
  { value: 'channel_admin', label: 'Channel Admin' },
  { value: 'tenant_admin', label: 'Owner / Admin da Empresa' },
  { value: 'tenant_manager', label: 'Gerente' },
  { value: 'tenant_viewer', label: 'Visualizador' },
  { value: 'tenant_kiosk', label: 'TV / Kiosk' },
];

function roleUsesScreenPermissions(role: string) {
  return role === 'tenant_manager' || role === 'tenant_viewer' || role === 'tenant_kiosk';
}

function menusForRole(role: string, tree: ScreenMenu[]): ScreenMenu[] {
  if (role === 'tenant_kiosk') return [];
  return tree.filter((m) => !m.kiosk_only);
}

function validPermissionKeysForRole(role: string, tree: ScreenMenu[]): Set<string> {
  const keys = new Set<string>();
  if (role === 'tenant_kiosk') {
    TV_SCREEN_OPTIONS.forEach((s) => keys.add(s.key));
    return keys;
  }
  for (const menu of menusForRole(role, tree)) {
    keys.add(menu.key);
    menu.panels.forEach((p) => keys.add(p.key));
  }
  if (role !== 'tenant_manager' && role !== 'tenant_viewer') {
    TV_SCREEN_OPTIONS.forEach((s) => keys.add(s.key));
  }
  return keys;
}

function roleRequiresBranch(role: string) {
  return role === 'tenant_manager' || role === 'tenant_viewer' || role === 'tenant_kiosk';
}

export default function PlatformUsersPage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [items, setItems] = useState<any[]>([]);
  const [companies, setCompanies] = useState<any[]>([]);
  const [channels, setChannels] = useState<any[]>([]);
  const [branchesMap, setBranchesMap] = useState<Record<string, any[]>>({});
  const [editingUserId, setEditingUserId] = useState<string | null>(null);
  const [form, setForm] = useState<any>(emptyUser());
  const [showFormPassword, setShowFormPassword] = useState(false);
  const [contactForm, setContactForm] = useState<any>(emptyContact());
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [screenTree, setScreenTree] = useState<ScreenMenu[]>(FALLBACK_SCREEN_TREE);

  async function load(session: any) {
    setLoading(true);
    try {
      const tasks: Promise<any>[] = [
        apiGet('/platform/users?limit=200'),
        apiGet('/platform/companies?limit=200'),
        apiGet('/platform/screen-registry').catch(() => null),
      ];
      if (session?.user_role === 'platform_master') {
        tasks.push(apiGet('/platform/channels?limit=200'));
      }
      const [usersRes, companiesRes, registryRes, channelsRes] = await Promise.all(tasks);
      setItems(usersRes?.items || []);
      setCompanies(companiesRes?.items || []);
      if (Array.isArray(registryRes?.menus) && registryRes.menus.length) {
        setScreenTree(registryRes.menus);
      }
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
      username: user.username || '',
      password: '',
      role: firstRole,
      is_enabled: Boolean(user.is_enabled),
      valid_from: toDateInput(user.valid_from),
      valid_until: toDateInput(user.valid_until),
      must_change_password: Boolean(user.must_change_password),
      locked_until: toDatetimeInput(user.locked_until),
      reset_failed_login: false,
      screen_permissions: Array.isArray(user.screen_permissions) ? user.screen_permissions : [],
      accesses: (user.accesses || []).length
        ? user.accesses.map((access: any) => ({
            role: access.role || firstRole,
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

    // Pre-load branches for companies in existing accesses
    const empresaIds: string[] = Array.from(new Set(
      (user.accesses || []).map((a: any) => a.id_empresa).filter(Boolean).map(String)
    ));
    for (const eid of empresaIds) {
      if (!branchesMap[eid]) {
        apiGet(`/platform/companies/${eid}`)
          .then((detail: any) => {
            setBranchesMap((prev) => ({ ...prev, [eid]: detail?.branches || [] }));
          })
          .catch(() => {
            setBranchesMap((prev) => ({ ...prev, [eid]: [] }));
          });
      }
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
  const isPlatformSuperuser = Boolean(me?.access?.platform_superuser);

  function setRole(role: string) {
    setForm((current: any) => {
      const validKeys = validPermissionKeysForRole(role, screenTree);
      let nextPerms = (current.screen_permissions || []).filter((k: string) => validKeys.has(k));
      // Troca para gerente/visualizador sem nada marcado → libera produto completo.
      if (
        (role === 'tenant_manager' || role === 'tenant_viewer')
        && nextPerms.length === 0
      ) {
        nextPerms = allProductPermissionKeys(screenTree);
      }
      if (role === 'tenant_kiosk' && nextPerms.length === 0) {
        nextPerms = TV_SCREEN_OPTIONS.map((s) => s.key);
      }
      return {
        ...current,
        role,
        screen_permissions: nextPerms,
        accesses:
          role === 'platform_admin' || role === 'platform_master' || role === 'product_global'
            ? [emptyAccess(role)]
            : current.accesses.map((access: any) => ({ ...access, role })),
      };
    });
  }

  function updateAccess(index: number, patch: any) {
    const accesses = [...form.accesses];
    accesses[index] = { ...accesses[index], ...patch, role: form.role };
    // When company changes, reset branch and load branches for that company
    if (patch.id_empresa !== undefined) {
      accesses[index].id_filial = '';
      const empresaId = patch.id_empresa;
      if (empresaId && !branchesMap[empresaId]) {
        apiGet(`/platform/companies/${empresaId}`)
          .then((detail: any) => {
            const branches = detail?.branches || [];
            setBranchesMap((prev) => ({ ...prev, [empresaId]: branches }));
          })
          .catch(() => {
            setBranchesMap((prev) => ({ ...prev, [empresaId]: [] }));
          });
      }
    }
    setForm({ ...form, accesses });
  }

  async function submit(event: FormEvent) {
    event.preventDefault();
    setError('');
    const usernameValidation = validateUsernameInput(form.username);
    if (!usernameValidation.ok) {
      setForm((current: any) => ({ ...current, username: usernameValidation.normalized }));
      setError(USERNAME_ERROR_MESSAGE);
      return;
    }
    if (roleRequiresBranch(form.role) && form.accesses.some((access: any) => !access.id_filial)) {
      setError('tenant_manager, tenant_viewer e tenant_kiosk exigem filial explícita em todos os vínculos.');
      return;
    }
    setSaving(true);
    try {
      const payload = {
        ...form,
        password: form.password || null,
        username: usernameValidation.normalized,
        valid_from: form.valid_from || null,
        valid_until: form.valid_until || null,
        locked_until: form.locked_until || null,
        screen_permissions: roleUsesScreenPermissions(form.role) ? form.screen_permissions : null,
        accesses:
          form.role === 'platform_admin' || form.role === 'platform_master' || form.role === 'product_global'
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
      const apiError = err?.response?.data?.error;
      const apiDetail = err?.response?.data?.detail;
      let apiMessage: string | undefined;
      if (Array.isArray(apiDetail)) {
        // Pydantic validation errors: extract user-friendly messages
        const msgs = apiDetail.map((e: any) => {
          const field = e?.loc?.slice(-1)?.[0] || '';
          const msg = e?.msg || '';
          if (field === 'password' && msg.includes('at least 8')) return 'Senha deve ter no mínimo 8 caracteres.';
          if (field === 'password') return `Senha: ${msg}`;
          if (field === 'email') return `Email: ${msg}`;
          if (field === 'username') return `Usuário: ${msg}`;
          if (field === 'nome') return `Nome: ${msg}`;
          return msg;
        });
        apiMessage = msgs.filter(Boolean).join(' ') || 'Erro de validação.';
      } else {
        apiMessage = apiDetail?.message;
      }
      if (apiError === 'username_conflict') {
        setError(apiMessage || 'Nome de usuário já está em uso.');
      } else if (apiError === 'email_conflict') {
        setError(apiMessage || 'Email já está em uso.');
      } else if (apiError === 'validation_error') {
        setError(apiMessage || 'Erro de validação.');
      } else if (apiMessage && String(apiMessage).includes('Nome de usuário')) {
        setError(apiMessage);
      } else {
        setError(apiMessage || 'Falha ao salvar usuário.');
      }
    } finally {
      setSaving(false);
    }
  }

  async function resetUserMfa() {
    if (!editingUserId) return;
    if (!window.confirm('Resetar o 2FA deste usuário? Ele precisará reconfigurar o autenticador no próximo acesso.')) return;
    setSaving(true);
    setError('');
    try {
      await api.post(`/platform/users/${editingUserId}/mfa-reset`);
      await load(me);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao resetar 2FA do usuário.');
    } finally {
      setSaving(false);
    }
  }

  async function requireUserMfa(required: boolean) {
    if (!editingUserId) return;
    setSaving(true);
    setError('');
    try {
      await api.post(`/platform/users/${editingUserId}/mfa-require`, { required });
      await load(me);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao atualizar exigência de 2FA.');
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
              <input className="input" placeholder="Email" value={form.email} onChange={(e) => setForm({ ...form, email: e.target.value })} />
              <input
                className="input"
                placeholder="Nome de usuário"
                value={form.username}
                onChange={(e) => setForm({ ...form, username: normalizeUsernameInput(e.target.value) })}
                pattern="[a-z0-9._-]{3,32}"
                minLength={3}
                maxLength={32}
                title="Use 3-32 caracteres com a-z, 0-9, ponto, underscore ou hífen."
                required
                autoCapitalize="none"
                autoCorrect="off"
                spellCheck={false}
              />
              <div style={{ position: 'relative' }}>
                <input
                  className="input"
                  type={showFormPassword ? "text" : "password"}
                  placeholder={editingUserId ? 'Nova senha opcional' : 'Senha inicial'}
                  value={form.password}
                  onChange={(e) => setForm({ ...form, password: e.target.value })}
                  style={{ paddingRight: 40 }}
                />
                <button
                  type="button"
                  onClick={() => setShowFormPassword(!showFormPassword)}
                  aria-label={showFormPassword ? "Ocultar senha" : "Mostrar senha"}
                  style={{
                    position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)',
                    background: 'none', border: 'none', cursor: 'pointer', padding: 4,
                    color: 'var(--muted, #94a3b8)', fontSize: 18, lineHeight: 1,
                  }}
                >
                  {showFormPassword ? '🙈' : '👁'}
                </button>
              </div>
              <select className="input" value={form.role} onChange={(e) => setRole(e.target.value)}>
                <option value="tenant_admin">tenant_admin</option>
                <option value="tenant_manager">tenant_manager</option>
                <option value="tenant_viewer">tenant_viewer</option>
                <option value="tenant_kiosk">tenant_kiosk (Vendedor/TV)</option>
                {me?.user_role === 'platform_master' ? <option value="channel_admin">channel_admin</option> : null}
                {me?.user_role === 'platform_master' ? <option value="product_global">product_global</option> : null}
                {me?.user_role === 'platform_master' ? <option value="platform_admin">platform_admin</option> : null}
                {isPlatformSuperuser || form.role === 'platform_master' ? (
                  <option value="platform_master" disabled={!isPlatformSuperuser}>
                    platform_master
                  </option>
                ) : null}
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

            <div className="platformFieldHint">Login por username exige 3-32 caracteres com apenas `a-z`, `0-9`, `.`, `_` ou `-`.</div>

            {form.role !== 'platform_admin' && form.role !== 'platform_master' && form.role !== 'product_global'
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
	                          <select
	                            className="input"
	                            value={access.id_filial}
	                            onChange={(e) => updateAccess(index, { id_filial: e.target.value })}
	                          >
	                            <option value="">{roleRequiresBranch(form.role) ? 'Filial obrigatória' : 'Filial opcional'}</option>
	                            {(branchesMap[access.id_empresa] || []).map((branch: any) => (
	                              <option key={branch.id_filial} value={branch.id_filial}>
	                                {branch.id_filial} - {branch.nome || `Filial ${branch.id_filial}`}
	                              </option>
	                            ))}
	                          </select>
	                          {access.id_empresa && !branchesMap[access.id_empresa] ? (
	                            <div className="platformFieldHint">Carregando filiais...</div>
	                          ) : access.id_empresa && branchesMap[access.id_empresa]?.length === 0 ? (
	                            <div className="platformFieldHint" style={{ color: 'var(--color-warning)' }}>Nenhuma filial encontrada para esta empresa.</div>
	                          ) : null}
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
                  <div className="platformFieldHint">Perfis internos e o usuário global de produto usam vínculo global único.</div>
                )}

            <div className="platformInlineFilters">
              {form.role !== 'platform_admin' && form.role !== 'platform_master' && form.role !== 'product_global' ? (
                <button className="btn" type="button" onClick={() => setForm({ ...form, accesses: [...form.accesses, emptyAccess(form.role)] })}>
                  Adicionar vínculo
                </button>
              ) : null}

              {roleUsesScreenPermissions(form.role) ? (
                <div style={{ width: '100%', marginBottom: 8 }}>
                  <div className="platformFieldHint" style={{ marginBottom: 4 }}>
                    Menus e painéis permitidos (default = tudo liberado; desmarque o que não pode ver):
                  </div>
                  {form.role === 'tenant_kiosk' ? (
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
                      {TV_SCREEN_OPTIONS.map((screen) => (
                        <label key={screen.key} className="platformCheckbox" style={{ minWidth: 160 }}>
                          <input
                            type="checkbox"
                            checked={form.screen_permissions.includes(screen.key)}
                            onChange={(e) => {
                              const perms = e.target.checked
                                ? [...form.screen_permissions, screen.key]
                                : form.screen_permissions.filter((k: string) => k !== screen.key);
                              setForm({ ...form, screen_permissions: perms });
                            }}
                          />
                          {screen.label}
                        </label>
                      ))}
                    </div>
                  ) : (
                    <div className="platformScreenTree">
                      {menusForRole(form.role, screenTree).map((menu) => {
                        const menuOn = form.screen_permissions.includes(menu.key);
                        return (
                          <div key={menu.key} className="platformScreenMenu">
                            <label className="platformCheckbox">
                              <input
                                type="checkbox"
                                checked={menuOn}
                                onChange={(e) =>
                                  setForm({
                                    ...form,
                                    screen_permissions: toggleMenuPermission(
                                      form.screen_permissions,
                                      menu,
                                      e.target.checked,
                                    ),
                                  })
                                }
                              />
                              <strong>{menu.label}</strong>
                            </label>
                            {menuOn && menu.panels.length > 0 ? (
                              <div className="platformScreenPanels">
                                {menu.panels.map((panel) => (
                                  <label key={panel.key} className="platformCheckbox">
                                    <input
                                      type="checkbox"
                                      checked={form.screen_permissions.includes(panel.key)}
                                      onChange={(e) =>
                                        setForm({
                                          ...form,
                                          screen_permissions: togglePanelPermission(
                                            form.screen_permissions,
                                            menu,
                                            panel.key,
                                            e.target.checked,
                                          ),
                                        })
                                      }
                                    />
                                    {panel.label}
                                  </label>
                                ))}
                              </div>
                            ) : null}
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
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
              {editingUserId ? (
                <button
                  className="btn"
                  type="button"
                  disabled={saving}
                  onClick={resetUserMfa}
                  style={{ background: 'transparent', border: '1px solid var(--border)' }}
                  title="Remove o 2FA do usuário; ele precisará reconfigurar no próximo acesso."
                >
                  Resetar 2FA
                </button>
              ) : null}
              {editingUserId ? (
                (() => {
                  const eu = items.find((u) => u.id === editingUserId);
                  const isRequired = !!eu?.totp_required;
                  return (
                    <button
                      className="btn"
                      type="button"
                      disabled={saving}
                      onClick={() => requireUserMfa(!isRequired)}
                      style={{ background: 'transparent', border: '1px solid var(--border)' }}
                      title={isRequired ? 'Deixar de exigir 2FA para este usuário.' : 'Exigir 2FA: o usuário será obrigado a configurar no próximo acesso.'}
                    >
                      {isRequired ? 'Não exigir 2FA' : 'Exigir 2FA'}
                    </button>
                  );
                })()
              ) : null}
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
                  {user.nome} - {user.username || user.email}
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
              <th>Username</th>
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
                <td>{item.username || '-'}</td>
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
                <td colSpan={9}>Nenhum usuário encontrado.</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </PlatformShell>
  );
}
