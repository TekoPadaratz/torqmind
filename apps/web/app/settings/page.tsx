'use client';

import { useState, useEffect, useCallback } from 'react';
import AppNav from '../components/AppNav';
import { apiGet, apiPatch, apiPost } from '../lib/api';
import { useScopeQuery } from '../lib/scope';

interface AlertCatalogItem {
  key: string;
  label: string;
}

interface TelegramConfig {
  telegram_chat_id: string | null;
  telegram_username: string | null;
  telegram_enabled: boolean;
  configured: boolean;
  bot_token_set: boolean;
  alert_catalog?: AlertCatalogItem[];
  alert_subscriptions?: Record<string, boolean>;
  company_prefs?: {
    preco_fixo_alerta_base?: string;
  };
}

const DEFAULT_CATALOG: AlertCatalogItem[] = [
  { key: 'VENDA_CANCELADA', label: 'Venda cancelada' },
  { key: 'NFE_INUTILIZADA', label: 'NFe inutilizada' },
  { key: 'CASH_OPEN_OVER_24H', label: 'Caixa aberto > 24h' },
  { key: 'PRECO_FIXO_BOMBA_DESATUALIZADO', label: 'Preço bomba × preço fixo' },
];

export default function SettingsPage() {
  const scope = useScopeQuery();
  const [config, setConfig] = useState<TelegramConfig | null>(null);
  const [chatId, setChatId] = useState('');
  const [username, setUsername] = useState('');
  const [enabled, setEnabled] = useState(false);
  const [subscriptions, setSubscriptions] = useState<Record<string, boolean>>({});
  const [precoBase, setPrecoBase] = useState<'venda' | 'custo'>('venda');
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [saveMsg, setSaveMsg] = useState<string | null>(null);
  const [testMsg, setTestMsg] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const loadConfig = useCallback(async () => {
    try {
      const params = new URLSearchParams();
      if (scope.id_empresa) params.set('id_empresa', String(scope.id_empresa));
      const qs = params.toString();
      const data = await apiGet(`/bi/me/telegram${qs ? `?${qs}` : ''}`);
      setConfig(data);
      setChatId(data.telegram_chat_id || '');
      setUsername(data.telegram_username || '');
      setEnabled(data.telegram_enabled ?? false);
      const catalog = data.alert_catalog?.length ? data.alert_catalog : DEFAULT_CATALOG;
      const subs: Record<string, boolean> = {};
      for (const item of catalog) {
        subs[item.key] = data.alert_subscriptions?.[item.key] ?? true;
      }
      setSubscriptions(subs);
      const base = String(data.company_prefs?.preco_fixo_alerta_base || 'venda');
      setPrecoBase(base === 'custo' ? 'custo' : 'venda');
    } catch (err: any) {
      setError(err?.message || 'Erro ao carregar configurações');
    }
  }, [scope.id_empresa]);

  useEffect(() => {
    loadConfig();
  }, [loadConfig]);

  async function handleSave(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    setSaveMsg(null);
    setError(null);
    try {
      await apiPatch('/bi/me/telegram', {
        telegram_chat_id: chatId.trim() || null,
        telegram_username: username.trim() || null,
        telegram_enabled: enabled,
        alert_subscriptions: subscriptions,
        preco_fixo_alerta_base: precoBase,
        id_empresa: scope.id_empresa || null,
      });
      setSaveMsg('Configurações salvas com sucesso.');
      await loadConfig();
    } catch (err: any) {
      setError(err?.message || 'Erro ao salvar configurações');
    } finally {
      setSaving(false);
    }
  }

  async function handleTest() {
    setTesting(true);
    setTestMsg(null);
    setError(null);
    try {
      const data = await apiPost('/bi/admin/telegram/test', {});
      if (data?.result?.sent) {
        setTestMsg('Mensagem de teste enviada com sucesso. Verifique seu Telegram.');
      } else {
        const reason = data?.result?.reason || 'unknown';
        setTestMsg(`Mensagem não enviada: ${reason}. Verifique se o Chat ID e o token do bot estão configurados.`);
      }
    } catch (err: any) {
      setError(err?.message || 'Erro ao enviar teste');
    } finally {
      setTesting(false);
    }
  }

  const catalog = config?.alert_catalog?.length ? config.alert_catalog : DEFAULT_CATALOG;

  return (
    <div>
      <AppNav title="Configurações" />
      <div className="container">
        <div className="bi-grid" style={{ marginTop: 12 }}>
          <div className="card col-12">
            <div className="sectionEyebrow">Preferências</div>
            <h2 style={{ marginTop: 4 }}>Notificações via Telegram</h2>
            <div className="muted" style={{ marginTop: 8 }}>
              Receba alertas críticos operacionais diretamente no seu Telegram.
              Escolha quais eventos deseja receber.
            </div>
          </div>

          {!config?.bot_token_set && (
            <div className="card col-12" style={{ borderColor: 'var(--color-warning)' }}>
              <strong>Bot não configurado.</strong>{' '}
              O token do bot Telegram não está ativo no servidor. Entre em contato com a equipe de TI para ativar as notificações.
            </div>
          )}

          <div className="card col-6">
            <h2>Configurar alertas</h2>
            <form onSubmit={handleSave} style={{ display: 'grid', gap: 16, marginTop: 16 }}>
              <div style={{ display: 'grid', gap: 6 }}>
                <label htmlFor="chatId" className="label">Chat ID do Telegram</label>
                <input
                  id="chatId"
                  className="input"
                  type="text"
                  placeholder="Ex: 123456789"
                  value={chatId}
                  onChange={(e) => setChatId(e.target.value)}
                />
                <span className="muted" style={{ fontSize: 12 }}>
                  Como obter: envie <code>/start</code> para{' '}
                  <a href="https://t.me/userinfobot" target="_blank" rel="noopener noreferrer" style={{ color: 'var(--accent-copper)' }}>
                    @userinfobot
                  </a>{' '}
                  no Telegram e cole o ID numérico aqui.
                </span>
              </div>

              <div style={{ display: 'grid', gap: 6 }}>
                <label htmlFor="username" className="label">Username do Telegram (opcional)</label>
                <input
                  id="username"
                  className="input"
                  type="text"
                  placeholder="Ex: @seunome"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                />
              </div>

              <label style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer' }}>
                <input
                  id="enabled"
                  type="checkbox"
                  checked={enabled}
                  onChange={(e) => setEnabled(e.target.checked)}
                  style={{ width: 18, height: 18, cursor: 'pointer' }}
                />
                Ativar alertas no Telegram
              </label>

              <div style={{ display: 'grid', gap: 8 }}>
                <div className="label">Quais alertas receber</div>
                {catalog.map((item) => (
                  <label
                    key={item.key}
                    style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer' }}
                  >
                    <input
                      type="checkbox"
                      checked={subscriptions[item.key] ?? true}
                      disabled={!enabled}
                      onChange={(e) =>
                        setSubscriptions((prev) => ({ ...prev, [item.key]: e.target.checked }))
                      }
                      style={{ width: 18, height: 18, cursor: 'pointer' }}
                    />
                    {item.label}
                  </label>
                ))}
              </div>

              <div style={{ display: 'grid', gap: 6 }}>
                <label htmlFor="precoBase" className="label">
                  Base do alerta preço fixo × bomba
                </label>
                <select
                  id="precoBase"
                  className="input"
                  value={precoBase}
                  onChange={(e) => setPrecoBase(e.target.value === 'custo' ? 'custo' : 'venda')}
                >
                  <option value="venda">Preço de venda (bomba)</option>
                  <option value="custo">Preço de custo (reposição)</option>
                </select>
                <span className="muted" style={{ fontSize: 12 }}>
                  Compara o valor fixo do cliente com a referência escolhida. Preferência da empresa.
                </span>
              </div>

              {saveMsg && <div className="muted" style={{ color: 'var(--color-positive)' }}>{saveMsg}</div>}
              {error && <div className="muted" style={{ color: 'var(--color-negative)' }}>{error}</div>}

              <div style={{ display: 'flex', gap: 10 }}>
                <button className="btn btnPrimary" type="submit" disabled={saving}>
                  {saving ? 'Salvando...' : 'Salvar'}
                </button>
                <button
                  className="btn"
                  type="button"
                  disabled={testing || !config?.bot_token_set}
                  onClick={handleTest}
                  title={!config?.bot_token_set ? 'Bot não configurado' : 'Enviar mensagem de teste'}
                >
                  {testing ? 'Enviando...' : 'Testar envio'}
                </button>
              </div>
            </form>

            {testMsg && (
              <div className="muted" style={{ marginTop: 12 }}>
                {testMsg}
              </div>
            )}
          </div>

          <div className="card col-6">
            <h2>Status atual</h2>
            {config ? (
              <div style={{ display: 'grid', gap: 12, marginTop: 16 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span className="muted">Chat ID configurado</span>
                  <strong>{config.telegram_chat_id || '—'}</strong>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span className="muted">Alertas ativos</span>
                  <strong style={{ color: config.telegram_enabled ? 'var(--color-positive)' : 'var(--color-negative)' }}>
                    {config.telegram_enabled ? 'Sim' : 'Não'}
                  </strong>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span className="muted">Token do bot</span>
                  <strong style={{ color: config.bot_token_set ? 'var(--color-positive)' : 'var(--color-negative)' }}>
                    {config.bot_token_set ? 'Configurado' : 'Não configurado'}
                  </strong>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span className="muted">Base preço fixo</span>
                  <strong>{precoBase === 'custo' ? 'Custo' : 'Venda (bomba)'}</strong>
                </div>
              </div>
            ) : (
              <div className="muted" style={{ marginTop: 16 }}>Carregando...</div>
            )}

            <div className="muted" style={{ marginTop: 24, fontSize: 13, lineHeight: 1.5 }}>
              <strong>O que você pode receber:</strong>
              <ul style={{ marginTop: 8, paddingLeft: 16 }}>
                <li>🚨 Venda cancelada</li>
                <li>📋 Nota inutilizada</li>
                <li>⏰ Caixa aberto há mais de 24h</li>
                <li>⛽ Preço da bomba/custo acima do preço fixo do cliente</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
