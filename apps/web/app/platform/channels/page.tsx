'use client';

import { FormEvent, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';

import PlatformShell from '../../components/PlatformShell';
import { api, apiGet } from '../../lib/api';
import { loadSession } from '../../lib/session';

export const dynamic = 'force-dynamic';

const emptyForm = {
  name: '',
  contact_name: '',
  email: '',
  phone: '',
  notes: '',
  is_enabled: true,
};

export default function PlatformChannelsPage() {
  const router = useRouter();
  const [me, setMe] = useState<any>(null);
  const [items, setItems] = useState<any[]>([]);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [form, setForm] = useState<any>(emptyForm);
  const [error, setError] = useState('');

  async function load(session: any) {
    const res = await apiGet('/platform/channels?limit=200');
    setItems(res?.items || []);
  }

  function resetForm() {
    setEditingId(null);
    setForm(emptyForm);
  }

  function selectChannel(item: any) {
    setEditingId(Number(item.id));
    setForm({
      name: item.name || '',
      contact_name: item.contact_name || '',
      email: item.email || '',
      phone: item.phone || '',
      notes: item.notes || '',
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
        setError(err?.message || 'Falha ao carregar canais.');
      }
    };
    boot();
  }, [router]);

  if (!me) return null;

  async function submit(event: FormEvent) {
    event.preventDefault();
    try {
      const payload = {
        ...form,
        contact_name: form.contact_name || null,
        email: form.email || null,
        phone: form.phone || null,
        notes: form.notes || null,
      };
      if (editingId) {
        await api.patch(`/platform/channels/${editingId}`, payload);
      } else {
        await api.post('/platform/channels', payload);
      }
      await load(me);
      resetForm();
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao salvar canal.');
    }
  }

  return (
    <PlatformShell
      title="Canais"
      subtitle="Cadastro e manutenção da carteira comercial com histórico preservado nas empresas via contrato ativo."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}

      <div className="platformGrid">
        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">{editingId ? 'Edição' : 'Cadastro'}</div>
              <h2>{editingId ? 'Atualizar canal' : 'Novo canal de vendas'}</h2>
            </div>
            {editingId ? (
              <button className="btn" type="button" onClick={resetForm}>
                Novo canal
              </button>
            ) : null}
          </div>
          <form className="platformFormGrid" onSubmit={submit}>
            <input className="input" placeholder="Nome do canal" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} />
            <input className="input" placeholder="Contato" value={form.contact_name} onChange={(e) => setForm({ ...form, contact_name: e.target.value })} />
            <input className="input" placeholder="Email" value={form.email} onChange={(e) => setForm({ ...form, email: e.target.value })} />
            <input className="input" placeholder="Telefone" value={form.phone} onChange={(e) => setForm({ ...form, phone: e.target.value })} />
            <input className="input" placeholder="Notas operacionais" value={form.notes} onChange={(e) => setForm({ ...form, notes: e.target.value })} />
            <label className="platformCheckbox">
              <input type="checkbox" checked={form.is_enabled} onChange={(e) => setForm({ ...form, is_enabled: e.target.checked })} />
              Canal habilitado
            </label>
            <button className="btn" type="submit">
              {editingId ? 'Salvar canal' : 'Criar canal'}
            </button>
          </form>
        </div>

        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Carteira</div>
              <h2>Resumo operacional</h2>
            </div>
          </div>
          <div className="platformSummaryList">
            <div>Total de canais: {items.length}</div>
            <div>Canais ativos: {items.filter((item) => item.is_enabled).length}</div>
            <div>Empresas vinculadas: {items.reduce((total, item) => total + Number(item.companies_count || 0), 0)}</div>
          </div>
        </div>
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
              <th>Status</th>
              <th>Ações</th>
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
                <td>{item.is_enabled ? 'ativo' : 'inativo'}</td>
                <td className="platformActionCell">
                  <button className="btn" type="button" onClick={() => selectChannel(item)}>
                    Editar
                  </button>
                </td>
              </tr>
            ))}
            {!items.length ? (
              <tr>
                <td colSpan={7}>Nenhum canal cadastrado.</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </PlatformShell>
  );
}
