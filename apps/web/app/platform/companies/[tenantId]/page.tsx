'use client';

import { FormEvent, useEffect, useState } from 'react';
import { useParams, useRouter } from 'next/navigation';

import PlatformShell from '../../../components/PlatformShell';
import { api, apiGet } from '../../../lib/api';
import { formatCurrency, formatDateOnly, formatDateTime } from '../../../lib/format';
import { loadSession } from '../../../lib/session';

export const dynamic = 'force-dynamic';

function toDateInput(value: any) {
  return value ? String(value).slice(0, 10) : '';
}

function buildBranchForm(branch: any) {
  return {
    id_filial: branch.id_filial,
    nome: branch.nome || '',
    cnpj: branch.cnpj || '',
    is_enabled: Boolean(branch.is_active),
    valid_from: toDateInput(branch.valid_from),
    valid_until: toDateInput(branch.valid_until),
    blocked_reason: branch.blocked_reason || '',
  };
}

export default function PlatformCompanyDetailPage() {
  const router = useRouter();
  const params = useParams();
  const tenantId = Number((params?.tenantId as string) || 0);

  const [me, setMe] = useState<any>(null);
  const [data, setData] = useState<any>(null);
  const [companyForm, setCompanyForm] = useState<any>(null);
  const [branchForm, setBranchForm] = useState<any>(null);
  const [selectedBranchId, setSelectedBranchId] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [branchSaving, setBranchSaving] = useState(false);
  const [error, setError] = useState('');
  const [branchError, setBranchError] = useState('');

  async function loadCompany(session: any, preferredBranchId?: number | null) {
    setLoading(true);
    try {
      const detail = await apiGet(`/platform/companies/${tenantId}`);
      setData(detail);
      setCompanyForm({
        nome: detail.nome || '',
        cnpj: detail.cnpj || '',
        is_enabled: Boolean(detail.is_active),
        valid_from: toDateInput(detail.valid_from),
        valid_until: toDateInput(detail.valid_until),
        status: detail.status || 'active',
        billing_status: detail.billing_status || 'current',
        suspended_reason: detail.suspended_reason || '',
        sales_history_days: String(detail.sales_history_days || 365),
        default_product_scope_days: String(detail.default_product_scope_days || 1),
      });
      const branches = detail.branches || [];
      const nextSelectedBranchId = preferredBranchId ?? selectedBranchId;
      const selectedBranch =
        branches.find((branch: any) => branch.id_filial === nextSelectedBranchId) ||
        branches[0] ||
        null;
      setSelectedBranchId(selectedBranch?.id_filial ?? null);
      setBranchForm(selectedBranch ? buildBranchForm(selectedBranch) : null);
      setBranchError('');
      setError('');
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao carregar empresa.');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    const boot = async () => {
      const session = await loadSession(router, 'platform');
      if (!session) return;
      setMe(session);
      await loadCompany(session);
    };
    boot();
  }, [router, tenantId]);

  if (!me) return null;

  if (loading && !companyForm) {
    return (
      <PlatformShell
        title={`Empresa #${tenantId}`}
        subtitle="Dados gerais, administração operacional das filiais existentes, usuários, contrato e trilha resumida de auditoria em uma única visão interna."
        me={me}
      >
        <div className="card">Carregando empresa...</div>
      </PlatformShell>
    );
  }

  if (!companyForm) return null;

  async function saveCompany(event: FormEvent) {
    event.preventDefault();
    setSaving(true);
    try {
      await api.patch(`/platform/companies/${tenantId}`, {
        ...companyForm,
        cnpj: companyForm.cnpj || null,
        valid_from: companyForm.valid_from || null,
        valid_until: companyForm.valid_until || null,
        suspended_reason: companyForm.suspended_reason || null,
        status: me?.access?.platform_finance ? companyForm.status : null,
        billing_status: me?.access?.platform_finance ? companyForm.billing_status : null,
        sales_history_days: Number(companyForm.sales_history_days || 365),
        default_product_scope_days: Number(companyForm.default_product_scope_days || 1),
      });
      await loadCompany(me);
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || 'Falha ao salvar empresa.');
    } finally {
      setSaving(false);
    }
  }

  function selectBranch(branch: any) {
    setSelectedBranchId(branch.id_filial);
    setBranchForm(buildBranchForm(branch));
    setBranchError('');
  }

  async function saveBranch(event: FormEvent) {
    event.preventDefault();
    if (!branchForm?.id_filial) return;
    setBranchSaving(true);
    setBranchError('');
    try {
      await api.patch(`/platform/companies/${tenantId}/branches/${branchForm.id_filial}`, {
        nome: branchForm.nome,
        cnpj: branchForm.cnpj || null,
        is_enabled: branchForm.is_enabled,
        valid_from: branchForm.valid_from || null,
        valid_until: branchForm.valid_until || null,
        blocked_reason: branchForm.blocked_reason || null,
      });
      await loadCompany(me, branchForm.id_filial);
    } catch (err: any) {
      setBranchError(err?.response?.data?.detail?.message || 'Falha ao salvar filial.');
    } finally {
      setBranchSaving(false);
    }
  }

  return (
    <PlatformShell
      title={`Empresa #${tenantId}`}
      subtitle="Dados gerais, administração operacional das filiais existentes, usuários, contrato e trilha resumida de auditoria em uma única visão interna."
      me={me}
    >
      {error ? <div className="card errorCard">{error}</div> : null}
      {data?.status && data.status !== 'active' ? (
        <div className="card">
          <div className="platformSectionEyebrow">Status comercial</div>
          <h2 style={{ marginTop: 8 }}>{data.status}</h2>
          <div className="platformFieldHint">{data.suspended_reason || 'A empresa está fora do ciclo comercial padrão e exige acompanhamento interno.'}</div>
        </div>
      ) : null}

      <div className="platformDetailGrid">
        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Dados gerais</div>
              <h2>{data?.nome}</h2>
            </div>
          </div>
          <form className="platformFormGrid" onSubmit={saveCompany}>
            <input className="input" value={companyForm.nome} onChange={(e) => setCompanyForm({ ...companyForm, nome: e.target.value })} />
            <input className="input" value={companyForm.cnpj} onChange={(e) => setCompanyForm({ ...companyForm, cnpj: e.target.value })} placeholder="CNPJ" />
            <input className="input" type="date" value={companyForm.valid_from} onChange={(e) => setCompanyForm({ ...companyForm, valid_from: e.target.value })} />
            <input className="input" type="date" value={companyForm.valid_until} onChange={(e) => setCompanyForm({ ...companyForm, valid_until: e.target.value })} />
            <input
              className="input"
              type="number"
              min={1}
              max={3650}
              value={companyForm.sales_history_days}
              onChange={(e) => setCompanyForm({ ...companyForm, sales_history_days: e.target.value })}
              placeholder="Histórico comercial (dias)"
            />
            <input
              className="input"
              type="number"
              min={1}
              max={365}
              value={companyForm.default_product_scope_days}
              onChange={(e) => setCompanyForm({ ...companyForm, default_product_scope_days: e.target.value })}
              placeholder="Janela padrão do produto"
            />
            {me?.access?.platform_finance ? (
              <>
                <select className="input" value={companyForm.status} onChange={(e) => setCompanyForm({ ...companyForm, status: e.target.value })}>
                  <option value="active">active</option>
                  <option value="trial">trial</option>
                  <option value="overdue">overdue</option>
                  <option value="grace">grace</option>
                  <option value="suspended_readonly">suspended_readonly</option>
                  <option value="suspended_total">suspended_total</option>
                  <option value="cancelled">cancelled</option>
                </select>
                <select className="input" value={companyForm.billing_status} onChange={(e) => setCompanyForm({ ...companyForm, billing_status: e.target.value })}>
                  <option value="current">current</option>
                  <option value="overdue">overdue</option>
                  <option value="grace">grace</option>
                  <option value="suspended">suspended</option>
                  <option value="cancelled">cancelled</option>
                </select>
                <input
                  className="input"
                  value={companyForm.suspended_reason}
                  onChange={(e) => setCompanyForm({ ...companyForm, suspended_reason: e.target.value })}
                  placeholder="Motivo da suspensão"
                />
              </>
            ) : (
              <div className="platformFieldHint">Status comercial e cobrança são geridos apenas pelo Master.</div>
            )}
            <div className="platformFieldHint" style={{ gridColumn: '1 / -1' }}>
              sales_history_days limita somente a trilha comercial curta. default_product_scope_days define o recorte automático de entrada no dashboard para usuários do produto.
            </div>
            <label className="platformCheckbox">
              <input
                type="checkbox"
                checked={companyForm.is_enabled}
                onChange={(e) => setCompanyForm({ ...companyForm, is_enabled: e.target.checked })}
              />
              Empresa habilitada
            </label>
            <button className="btn" type="submit" disabled={saving}>
              {saving ? 'Salvando...' : 'Salvar dados'}
            </button>
          </form>

          <div className="platformMetaStrip">
            <div>
              <div className="platformMetaLabel">Canal</div>
              <div className="platformMetaValue">{data?.channel_name || '-'}</div>
            </div>
            <div>
              <div className="platformMetaLabel">Plano</div>
              <div className="platformMetaValue">{data?.plan_name || '-'}</div>
            </div>
            <div>
              <div className="platformMetaLabel">Mensalidade</div>
              <div className="platformMetaValue">{data?.monthly_amount != null ? formatCurrency(data.monthly_amount) : '-'}</div>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Filiais</div>
              <h2>Administração operacional</h2>
            </div>
          </div>
          <div className="platformFieldHint">
            O cadastro-base continua vindo da Xpert pelo par oficial `id_empresa` + `id_filial`.
            {' '}
            A criação manual segue bloqueada, mas nome administrativo, CNPJ, vigência e estado da filial existente podem ser ajustados aqui sem o ETL reverter essas alterações.
          </div>

          {branchError ? <div className="card errorCard" style={{ marginTop: 16 }}>{branchError}</div> : null}

          <table className="table compact">
            <thead>
              <tr>
                <th>ID</th>
                <th>Nome</th>
                <th>Habilitada</th>
                <th>Vigência</th>
                <th>Bloqueio</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {(data?.branches || []).map((branch: any) => (
                <tr key={branch.id_filial}>
                  <td>{branch.id_filial}</td>
                  <td>{branch.nome}</td>
                  <td>{branch.is_active ? 'Sim' : 'Não'}</td>
                  <td>{formatDateOnly(branch.valid_until || branch.valid_from)}</td>
                  <td>{branch.blocked_reason || '-'}</td>
                  <td>
                    <button className="btn" type="button" onClick={() => selectBranch(branch)} disabled={branchSaving || loading}>
                      {selectedBranchId === branch.id_filial ? 'Editando' : 'Editar'}
                    </button>
                  </td>
                </tr>
              ))}
              {!(data?.branches || []).length ? (
                <tr>
                  <td colSpan={6}>Nenhuma filial sincronizada ainda para esta empresa.</td>
                </tr>
              ) : null}
            </tbody>
          </table>

          {branchForm ? (
            <>
              <div style={{ height: 16 }} />
              <div className="platformSectionHead">
                <div>
                  <div className="platformSectionEyebrow">Filial selecionada</div>
                  <h2>Filial #{branchForm.id_filial}</h2>
                </div>
              </div>
              <form className="platformFormGrid" onSubmit={saveBranch}>
                <input className="input" value={branchForm.nome} onChange={(e) => setBranchForm({ ...branchForm, nome: e.target.value })} />
                <input className="input" value={branchForm.cnpj} onChange={(e) => setBranchForm({ ...branchForm, cnpj: e.target.value })} placeholder="CNPJ" />
                <input className="input" type="date" value={branchForm.valid_from} onChange={(e) => setBranchForm({ ...branchForm, valid_from: e.target.value })} />
                <input className="input" type="date" value={branchForm.valid_until} onChange={(e) => setBranchForm({ ...branchForm, valid_until: e.target.value })} />
                <input
                  className="input"
                  value={branchForm.blocked_reason}
                  onChange={(e) => setBranchForm({ ...branchForm, blocked_reason: e.target.value })}
                  placeholder="Motivo do bloqueio"
                />
                <label className="platformCheckbox">
                  <input
                    type="checkbox"
                    checked={branchForm.is_enabled}
                    onChange={(e) => setBranchForm({ ...branchForm, is_enabled: e.target.checked })}
                  />
                  Filial habilitada
                </label>
                <button className="btn" type="submit" disabled={branchSaving}>
                  {branchSaving ? 'Salvando...' : 'Salvar filial'}
                </button>
              </form>
              {loading ? <div className="platformFieldHint">Recarregando dados atualizados da filial...</div> : null}
            </>
          ) : null}
        </div>
      </div>

      <div style={{ height: 16 }} />

      <div className="platformGrid">
        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Usuários</div>
              <h2>Equipe vinculada</h2>
            </div>
          </div>
          <table className="table compact">
            <thead>
              <tr>
                <th>Nome</th>
                <th>Email</th>
                <th>Papel</th>
                <th>Telegram</th>
              </tr>
            </thead>
            <tbody>
              {(data?.users || []).map((user: any) => (
                <tr key={user.id}>
                  <td>{user.nome}</td>
                  <td>{user.email}</td>
                  <td>{user.role}</td>
                  <td>{user.telegram_configured ? 'OK' : '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Contrato</div>
              <h2>Resumo comercial</h2>
            </div>
          </div>
          {data?.contract ? (
            <div className="platformSummaryList">
              <div>Plano: {data.contract.plan_name}</div>
              <div>Mensalidade: {formatCurrency(data.contract.monthly_amount)}</div>
              <div>Emissão: dia {data.contract.issue_day}</div>
              <div>Vencimento: dia {data.contract.billing_day}</div>
              <div>Canal: {data.contract.channel_name || '-'}</div>
              <div>1º ano: {data.contract.commission_first_year_pct}%</div>
              <div>Recorrente: {data.contract.commission_recurring_pct}%</div>
            </div>
          ) : (
            <div className="platformFieldHint">Sem contrato disponível para este perfil ou empresa.</div>
          )}
        </div>
      </div>

      <div style={{ height: 16 }} />

      <div className="platformGrid">
        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Notificações</div>
              <h2>Assinaturas</h2>
            </div>
          </div>
          <table className="table compact">
            <thead>
              <tr>
                <th>Usuário</th>
                <th>Evento</th>
                <th>Canal</th>
                <th>Severity</th>
              </tr>
            </thead>
            <tbody>
              {(data?.notification_subscriptions || []).map((item: any) => (
                <tr key={item.id}>
                  <td>{item.user_name}</td>
                  <td>{item.event_type}</td>
                  <td>{item.channel}</td>
                  <td>{item.severity_min || '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="card">
          <div className="platformSectionHead">
            <div>
              <div className="platformSectionEyebrow">Histórico</div>
              <h2>Auditoria resumida</h2>
            </div>
          </div>
          <table className="table compact">
            <thead>
              <tr>
                <th>Quando</th>
                <th>Ação</th>
                <th>Entidade</th>
              </tr>
            </thead>
            <tbody>
              {(data?.audit || []).map((item: any) => (
                <tr key={item.id}>
                  <td>{formatDateTime(item.created_at)}</td>
                  <td>{item.action}</td>
                  <td>{item.entity_type}</td>
                </tr>
              ))}
              {!data?.audit?.length ? (
                <tr>
                  <td colSpan={3}>Sem eventos recentes para este perfil.</td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>
    </PlatformShell>
  );
}
