'use client';

import { FormEvent, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../../components/PlatformShell';
import { apiGet, apiPatch, apiPost } from '../../lib/api';
import { loadSession } from '../../lib/session';

export const dynamic = 'force-dynamic';

type SmtpStatus = {
  configured?: boolean;
  source?: string;
  enabled?: boolean;
  host?: string | null;
  port?: number | null;
  use_ssl?: boolean;
  use_tls?: boolean;
  from_email?: string | null;
  from_name?: string | null;
  user?: string | null;
  user_configured?: boolean;
  password_configured?: boolean;
  timeout_seconds?: number | null;
};

type EmailForm = {
  channel_name: string;
  contact_name: string;
  from_email: string;
  smtp_enabled: boolean;
  smtp_host: string;
  smtp_port: string;
  smtp_user: string;
  smtp_password: string;
  smtp_use_ssl: boolean;
  smtp_use_tls: boolean;
  smtp_from_name: string;
  smtp_timeout_seconds: string;
};

const emptyForm = (): EmailForm => ({
  channel_name: 'TorqMind',
  contact_name: '',
  from_email: '',
  smtp_enabled: false,
  smtp_host: '',
  smtp_port: '587',
  smtp_user: '',
  smtp_password: '',
  smtp_use_ssl: false,
  smtp_use_tls: true,
  smtp_from_name: 'TorqMind',
  smtp_timeout_seconds: '20',
});

export default function PlatformEmailPage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [smtp, setSmtp] = useState<SmtpStatus>({});
  const [form, setForm] = useState<EmailForm>(emptyForm);
  const [error, setError] = useState('');
  const [info, setInfo] = useState('');
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);

  async function load() {
    const res = await apiGet('/platform/email');
    const profile = res?.profile || {};
    const status = (res?.smtp || {}) as SmtpStatus;
    setSmtp(status);
    setForm({
      channel_name: profile.channel_name || status.from_name || 'TorqMind',
      contact_name: profile.contact_name || '',
      from_email: profile.from_email || status.from_email || '',
      smtp_enabled: Boolean(status.enabled ?? profile.smtp_enabled),
      smtp_host: status.host || profile.smtp_host || '',
      smtp_port: String(status.port ?? profile.smtp_port ?? 587),
      smtp_user: status.user || profile.smtp_user || '',
      smtp_password: '',
      smtp_use_ssl: Boolean(status.use_ssl ?? profile.smtp_use_ssl),
      smtp_use_tls: Boolean(status.use_tls ?? profile.smtp_use_tls ?? true),
      smtp_from_name: status.from_name || profile.smtp_from_name || 'TorqMind',
      smtp_timeout_seconds: String(status.timeout_seconds ?? profile.smtp_timeout_seconds ?? 20),
    });
  }

  useEffect(() => {
    const boot = async () => {
      const session = await loadSession(router, 'platform');
      if (!session) return;
      setMe(session);
      try {
        await load();
      } catch (err: any) {
        setError(err?.message || 'Falha ao carregar e-mail da plataforma.');
      }
    };
    boot();
  }, [router]);

  if (!me) return null;

  async function submit(event: FormEvent) {
    event.preventDefault();
    setError('');
    setInfo('');
    setSaving(true);
    try {
      const payload: Record<string, unknown> = {
        channel_name: form.channel_name.trim(),
        contact_name: form.contact_name.trim() || null,
        from_email: form.from_email.trim() || null,
        smtp_enabled: form.smtp_enabled,
        smtp_host: form.smtp_host.trim() || null,
        smtp_port: Number(form.smtp_port) || 587,
        smtp_user: form.smtp_user.trim() || null,
        smtp_use_ssl: form.smtp_use_ssl,
        smtp_use_tls: form.smtp_use_tls,
        smtp_from_name: form.smtp_from_name.trim() || form.channel_name.trim() || 'TorqMind',
        smtp_timeout_seconds: Number(form.smtp_timeout_seconds) || 20,
      };
      const pwd = form.smtp_password.trim();
      if (pwd) payload.smtp_password = pwd;

      await apiPatch('/platform/email', payload);
      await load();
      setInfo('Configuração de e-mail salva.');
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || err?.message || 'Falha ao salvar.');
    } finally {
      setSaving(false);
    }
  }

  async function testSend() {
    setError('');
    setInfo('');
    setTesting(true);
    try {
      const res = await apiPost('/platform/email/test', {});
      if (res?.ok) {
        setInfo(res.message || 'E-mail de teste enviado.');
      } else {
        setError(res?.message || 'Falha no envio de teste.');
      }
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || err?.message || 'Falha no envio de teste.');
    } finally {
      setTesting(false);
    }
  }

  const passwordHint = smtp.password_configured
    ? 'Senha já gravada — deixe em branco para manter.'
    : 'Informe a senha SMTP.';

  return (
    <PlatformShell
      title="E-mail"
      subtitle="Canal de notificações administrativas (recuperação de senha e avisos)."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}
      {info ? <div className="card">{info}</div> : null}

      <div className="card">
        <div className="platformSectionHead">
          <div>
            <div className="platformSectionEyebrow">Configuração</div>
            <h2>E-mail e SMTP</h2>
          </div>
          <div className="muted" style={{ fontSize: 13 }}>
            Status: {smtp.configured ? 'pronto para enviar' : 'incompleto'}
            {smtp.source ? ` · origem ${smtp.source === 'database' ? 'salva' : 'ambiente'}` : ''}
          </div>
        </div>

        <form className="platformFormGrid" onSubmit={submit}>
          <label className="muted" style={{ fontSize: 12 }}>
            Nome do canal
            <input
              className="input"
              value={form.channel_name}
              onChange={(e) => setForm({ ...form, channel_name: e.target.value })}
              required
            />
          </label>
          <label className="muted" style={{ fontSize: 12 }}>
            Contato
            <input
              className="input"
              value={form.contact_name}
              onChange={(e) => setForm({ ...form, contact_name: e.target.value })}
            />
          </label>
          <label className="muted" style={{ fontSize: 12 }}>
            Remetente (From)
            <input
              className="input"
              type="email"
              value={form.from_email}
              onChange={(e) => setForm({ ...form, from_email: e.target.value })}
            />
          </label>
          <label className="muted" style={{ fontSize: 12 }}>
            Nome do remetente
            <input
              className="input"
              value={form.smtp_from_name}
              onChange={(e) => setForm({ ...form, smtp_from_name: e.target.value })}
            />
          </label>

          <label className="muted" style={{ fontSize: 12, display: 'flex', alignItems: 'center', gap: 8 }}>
            <input
              type="checkbox"
              checked={form.smtp_enabled}
              onChange={(e) => setForm({ ...form, smtp_enabled: e.target.checked })}
            />
            Envio SMTP habilitado
          </label>

          <label className="muted" style={{ fontSize: 12 }}>
            Host SMTP
            <input
              className="input"
              value={form.smtp_host}
              onChange={(e) => setForm({ ...form, smtp_host: e.target.value })}
              placeholder="mail.exemplo.com.br"
            />
          </label>
          <label className="muted" style={{ fontSize: 12 }}>
            Porta
            <input
              className="input"
              type="number"
              min={1}
              max={65535}
              value={form.smtp_port}
              onChange={(e) => setForm({ ...form, smtp_port: e.target.value })}
            />
          </label>
          <label className="muted" style={{ fontSize: 12 }}>
            Usuário SMTP
            <input
              className="input"
              value={form.smtp_user}
              onChange={(e) => setForm({ ...form, smtp_user: e.target.value })}
              autoComplete="off"
            />
          </label>
          <label className="muted" style={{ fontSize: 12 }}>
            Senha SMTP
            <input
              className="input"
              type="password"
              value={form.smtp_password}
              onChange={(e) => setForm({ ...form, smtp_password: e.target.value })}
              placeholder={passwordHint}
              autoComplete="new-password"
            />
          </label>
          <label className="muted" style={{ fontSize: 12 }}>
            Timeout (segundos)
            <input
              className="input"
              type="number"
              min={5}
              max={120}
              value={form.smtp_timeout_seconds}
              onChange={(e) => setForm({ ...form, smtp_timeout_seconds: e.target.value })}
            />
          </label>

          <label className="muted" style={{ fontSize: 12, display: 'flex', alignItems: 'center', gap: 8 }}>
            <input
              type="checkbox"
              checked={form.smtp_use_ssl}
              onChange={(e) =>
                setForm({
                  ...form,
                  smtp_use_ssl: e.target.checked,
                  smtp_use_tls: e.target.checked ? false : form.smtp_use_tls,
                })
              }
            />
            SSL (ex.: porta 465)
          </label>
          <label className="muted" style={{ fontSize: 12, display: 'flex', alignItems: 'center', gap: 8 }}>
            <input
              type="checkbox"
              checked={form.smtp_use_tls}
              onChange={(e) =>
                setForm({
                  ...form,
                  smtp_use_tls: e.target.checked,
                  smtp_use_ssl: e.target.checked ? false : form.smtp_use_ssl,
                })
              }
            />
            STARTTLS (ex.: porta 587)
          </label>

          <button className="btn" type="submit" disabled={saving}>
            {saving ? 'Salvando…' : 'Salvar'}
          </button>
          <button className="btn" type="button" onClick={testSend} disabled={testing}>
            {testing ? 'Enviando…' : 'Testar envio'}
          </button>
        </form>
        <p className="muted" style={{ marginTop: 12, fontSize: 13 }}>
          O teste envia para o e-mail da sua conta ({me?.email || '—'}).
        </p>
      </div>
    </PlatformShell>
  );
}
