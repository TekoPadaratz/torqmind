'use client';

import { FormEvent, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../../components/PlatformShell';
import { api, apiGet } from '../../lib/api';
import { formatDateTime } from '../../lib/format';
import { loadSession } from '../../lib/session';

export const dynamic = 'force-dynamic';

export default function PlatformNotificationsPage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [users, setUsers] = useState<any[]>([]);
  const [companies, setCompanies] = useState<any[]>([]);
  const [subscriptions, setSubscriptions] = useState<any[]>([]);
  const [editingSubscriptionId, setEditingSubscriptionId] = useState<number | null>(null);
  const [contactForm, setContactForm] = useState<any>({
    user_id: '',
    telegram_chat_id: '',
    telegram_username: '',
    telegram_enabled: false,
    email: '',
    phone: '',
  });
  const [subscriptionForm, setSubscriptionForm] = useState<any>({
    user_id: '',
    tenant_id: '',
    branch_id: '',
    event_type: 'payment_overdue',
    channel: 'telegram',
    severity_min: 'WARN',
    is_enabled: true,
  });
  const [error, setError] = useState('');

  async function load(session: any) {
    const [usersRes, companiesRes, subscriptionsRes] = await Promise.all([
      apiGet('/platform/users?limit=200'),
      apiGet('/platform/companies?limit=200'),
      apiGet('/platform/notifications/subscriptions?limit=200'),
    ]);
    setUsers(usersRes?.items || []);
    setCompanies(companiesRes?.items || []);
    setSubscriptions(subscriptionsRes?.items || []);
    if (!contactForm.user_id && usersRes?.items?.length) {
      const first = usersRes.items[0];
      setContactForm({
        user_id: first.id,
        telegram_chat_id: first.telegram_chat_id || '',
        telegram_username: first.telegram_username || '',
        telegram_enabled: Boolean(first.telegram_enabled),
        email: first.contact_email || first.email || '',
        phone: first.contact_phone || '',
      });
      setSubscriptionForm((current: any) => ({ ...current, user_id: first.id }));
    }
  }

  function applyUserToContact(userId: string) {
    const selectedUser = users.find((item) => item.id === userId);
    setContactForm({
      user_id: userId,
      telegram_chat_id: selectedUser?.telegram_chat_id || '',
      telegram_username: selectedUser?.telegram_username || '',
      telegram_enabled: Boolean(selectedUser?.telegram_enabled),
      email: selectedUser?.contact_email || selectedUser?.email || '',
      phone: selectedUser?.contact_phone || '',
    });
  }

  function selectSubscription(item: any) {
    setEditingSubscriptionId(Number(item.id));
    setSubscriptionForm({
      user_id: item.user_id,
      tenant_id: item.tenant_id ? String(item.tenant_id) : '',
      branch_id: item.branch_id ? String(item.branch_id) : '',
      event_type: item.event_type,
      channel: item.channel,
      severity_min: item.severity_min || '',
      is_enabled: Boolean(item.is_enabled),
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
        setError(err?.message || 'Falha ao carregar notificações.');
      }
    };
    boot();
  }, [router]);

  if (!me) return null;

  async function saveContacts(event: FormEvent) {
    event.preventDefault();
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
      setError(err?.response?.data?.detail?.message || 'Falha ao salvar contatos.');
    }
  }

  async function saveSubscription(event: FormEvent) {
    event.preventDefault();
    try {
      const payload = {
        user_id: subscriptionForm.user_id,
        tenant_id: subscriptionForm.tenant_id ? Number(subscriptionForm.tenant_id) : null,
        branch_id: subscriptionForm.branch_id ? Number(subscriptionForm.branch_id) : null,
        event_type: subscriptionForm.event_type,
        channel: subscriptionForm.channel,
        severity_min: subscriptionForm.severity_min || null,
        is_enabled: subscriptionForm.is_enabled,
      };
      if (editingSubscriptionId) {
        await api.patch(`/platform/notifications/subscriptions/${editingSubscriptionId}`, payload);
      } else {
        await api.post('/platform/notifications/subscriptions', payload);
      }
      setEditingSubscriptionId(null);
      await load(me);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao salvar assinatura.');
    }
  }

  return (
    <PlatformShell
      title="Telegram e notificações"
      subtitle="Configuração por usuário e por escopo, sem depender de um único Telegram global para toda a operação."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}

      <div className="platformGrid">
        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Contato</div>
              <h2>Telegram por usuário</h2>
            </div>
          </div>
          <form className="platformFormGrid" onSubmit={saveContacts}>
            <select className="input" value={contactForm.user_id} onChange={(e) => applyUserToContact(e.target.value)}>
              {users.map((user) => (
                <option key={user.id} value={user.id}>
                  {user.nome} - {user.email}
                </option>
              ))}
            </select>
            <input className="input" placeholder="telegram_chat_id" value={contactForm.telegram_chat_id} onChange={(e) => setContactForm({ ...contactForm, telegram_chat_id: e.target.value })} />
            <input className="input" placeholder="@username" value={contactForm.telegram_username} onChange={(e) => setContactForm({ ...contactForm, telegram_username: e.target.value })} />
            <input className="input" placeholder="email" value={contactForm.email} onChange={(e) => setContactForm({ ...contactForm, email: e.target.value })} />
            <input className="input" placeholder="telefone" value={contactForm.phone} onChange={(e) => setContactForm({ ...contactForm, phone: e.target.value })} />
            <label className="platformCheckbox">
              <input type="checkbox" checked={contactForm.telegram_enabled} onChange={(e) => setContactForm({ ...contactForm, telegram_enabled: e.target.checked })} />
              Telegram habilitado
            </label>
            <button className="btn" type="submit">
              Salvar contato
            </button>
          </form>
        </div>

        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Assinatura</div>
              <h2>{editingSubscriptionId ? 'Editar regra' : 'Nova regra'}</h2>
            </div>
            {editingSubscriptionId ? (
              <button
                className="btn"
                type="button"
                onClick={() => {
                  setEditingSubscriptionId(null);
                  setSubscriptionForm({
                    user_id: users[0]?.id || '',
                    tenant_id: '',
                    branch_id: '',
                    event_type: 'payment_overdue',
                    channel: 'telegram',
                    severity_min: 'WARN',
                    is_enabled: true,
                  });
                }}
              >
                Nova regra
              </button>
            ) : null}
          </div>
          <form className="platformFormGrid" onSubmit={saveSubscription}>
            <select className="input" value={subscriptionForm.user_id} onChange={(e) => setSubscriptionForm({ ...subscriptionForm, user_id: e.target.value })}>
              {users.map((user) => (
                <option key={user.id} value={user.id}>
                  {user.nome}
                </option>
              ))}
            </select>
            <select className="input" value={subscriptionForm.tenant_id} onChange={(e) => setSubscriptionForm({ ...subscriptionForm, tenant_id: e.target.value })}>
              <option value="">Todas as empresas</option>
              {companies.map((company) => (
                <option key={company.id_empresa} value={company.id_empresa}>
                  {company.nome}
                </option>
              ))}
            </select>
            <input className="input" placeholder="Filial opcional" value={subscriptionForm.branch_id} onChange={(e) => setSubscriptionForm({ ...subscriptionForm, branch_id: e.target.value })} />
            <input className="input" placeholder="Evento" value={subscriptionForm.event_type} onChange={(e) => setSubscriptionForm({ ...subscriptionForm, event_type: e.target.value })} />
            <select className="input" value={subscriptionForm.channel} onChange={(e) => setSubscriptionForm({ ...subscriptionForm, channel: e.target.value })}>
              <option value="telegram">telegram</option>
              <option value="email">email</option>
              <option value="phone">phone</option>
              <option value="in_app">in_app</option>
            </select>
            <select className="input" value={subscriptionForm.severity_min} onChange={(e) => setSubscriptionForm({ ...subscriptionForm, severity_min: e.target.value })}>
              <option value="INFO">INFO</option>
              <option value="WARN">WARN</option>
              <option value="CRITICAL">CRITICAL</option>
            </select>
            <label className="platformCheckbox">
              <input type="checkbox" checked={subscriptionForm.is_enabled} onChange={(e) => setSubscriptionForm({ ...subscriptionForm, is_enabled: e.target.checked })} />
              Assinatura ativa
            </label>
            <button className="btn" type="submit">
              {editingSubscriptionId ? 'Salvar assinatura' : 'Criar assinatura'}
            </button>
          </form>
        </div>
      </div>

      <div style={{ height: 16 }} />

      <div className="card">
        <div className="platformSectionHead">
          <div>
            <div className="platformSectionEyebrow">Mapa atual</div>
            <h2>Assinaturas configuradas</h2>
          </div>
        </div>
        <table className="table">
          <thead>
            <tr>
              <th>Usuário</th>
              <th>Empresa</th>
              <th>Evento</th>
              <th>Canal</th>
              <th>Severity</th>
              <th>Criado em</th>
              <th>Ações</th>
            </tr>
          </thead>
          <tbody>
            {subscriptions.map((item) => (
              <tr key={item.id}>
                <td>{item.user_name}</td>
                <td>{item.tenant_name || 'Global'}</td>
                <td>{item.event_type}</td>
                <td>{item.channel}</td>
                <td>{item.severity_min || '-'}</td>
                <td>{formatDateTime(item.created_at)}</td>
                <td className="platformActionCell">
                  <button className="btn" type="button" onClick={() => selectSubscription(item)}>
                    Editar
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
