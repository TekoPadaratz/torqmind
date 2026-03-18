'use client';

import { FormEvent, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../../components/PlatformShell';
import { apiGet, apiPost } from '../../lib/api';
import { loadSession } from '../../lib/session';

export const dynamic = 'force-dynamic';

export default function PlatformChannelsPage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [items, setItems] = useState<any[]>([]);
  const [form, setForm] = useState({ name: '', contact_name: '', email: '', phone: '', notes: '', is_enabled: true });
  const [error, setError] = useState('');

  async function load(session: any) {
    const res = await apiGet('/platform/channels?limit=200');
    setItems(res?.items || []);
  }

  useEffect(() => {
    const boot = async () => {
      const session = await loadSession(router, 'platform');
      if (!session) return;
      setMe(session);
      try {
        await load(session);
      } catch (err: any) {
        setError(err?.message || 'Falha ao carregar canais.');
      }
    };
    boot();
  }, [router]);

  if (!me) return null;

  async function submit(event: FormEvent) {
    event.preventDefault();
    try {
      await apiPost('/platform/channels', {
        ...form,
        contact_name: form.contact_name || null,
        email: form.email || null,
        phone: form.phone || null,
        notes: form.notes || null,
      });
      setForm({ name: '', contact_name: '', email: '', phone: '', notes: '', is_enabled: true });
      await load(me);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao salvar canal.');
    }
  }

  return (
    <PlatformShell
      title="Canais"
      subtitle="Cadastro de canais de venda, base para carteira comercial e repasse de comissão."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}

      <div className="card">
        <form className="platformFormGrid" onSubmit={submit}>
          <input className="input" placeholder="Nome do canal" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} />
          <input className="input" placeholder="Contato" value={form.contact_name} onChange={(e) => setForm({ ...form, contact_name: e.target.value })} />
          <input className="input" placeholder="Email" value={form.email} onChange={(e) => setForm({ ...form, email: e.target.value })} />
          <input className="input" placeholder="Telefone" value={form.phone} onChange={(e) => setForm({ ...form, phone: e.target.value })} />
          <input className="input" placeholder="Notas" value={form.notes} onChange={(e) => setForm({ ...form, notes: e.target.value })} />
          <button className="btn" type="submit">
            Criar canal
          </button>
        </form>
      </div>

      <div style={{ height: 16 }} />

      <div className="card">
        <table className="table">
          <thead>
            <tr>
              <th>ID</th>
              <th>Canal</th>
              <th>Contato</th>
              <th>Email</th>
              <th>Empresas</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.id}>
                <td>{item.id}</td>
                <td>{item.name}</td>
                <td>{item.contact_name || '-'}</td>
                <td>{item.email || '-'}</td>
                <td>{item.companies_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </PlatformShell>
  );
}
