'use client';

import { useState, useEffect, useCallback } from 'react';
import AppNav from '../components/AppNav';
import { apiGet, apiPatch, apiPost } from '../lib/api';

interface TelegramConfig {
  telegram_chat_id: string | null;
  telegram_username: string | null;
  telegram_enabled: boolean;
  configured: boolean;
  bot_token_set: boolean;
}

export default function SettingsPage() {
  const [config, setConfig] = useState<TelegramConfig | null>(null);
  const [chatId, setChatId] = useState('');
  const [username, setUsername] = useState('');
  const [enabled, setEnabled] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [saveMsg, setSaveMsg] = useState<string | null>(null);
  const [testMsg, setTestMsg] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const loadConfig = useCallback(async () => {
    try {
      const data = await apiGet('/bi/me/telegram');
      setConfig(data);
      setChatId(data.telegram_chat_id || '');
      setUsername(data.telegram_username || '');
      setEnabled(data.telegram_enabled ?? false);
    } catch (err: any) {
      setError(err?.message || 'Erro ao carregar configurações');
    }
  }, []);

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

  return (
    <AppNav title="Configurações">
      <div className="pageContent">
        <div className="pageHeader">
          <h1 className="pageTitle">Configurações</h1>
          <p className="muted">Gerencie suas preferências de notificações.</p>
        </div>

        <section className="card" style={{ maxWidth: 560 }}>
          <h2 className="cardTitle" style={{ marginBottom: 8 }}>Alertas via Telegram</h2>
          <p className="muted" style={{ marginBottom: 16 }}>
            Receba alertas críticos operacionais diretamente no seu Telegram.
          </p>

          {!config?.bot_token_set && (
            <div className="alertBanner alertBannerWarn" style={{ marginBottom: 16 }}>
              O token do bot Telegram não está configurado no servidor. Entre em contato com a equipe de TI para ativar as notificações.
            </div>
          )}

          <form onSubmit={handleSave}>
            <div className="formField">
              <label htmlFor="chatId" className="formLabel">
                Chat ID do Telegram
              </label>
              <input
                id="chatId"
                className="input"
                type="text"
                placeholder="Ex: 123456789"
                value={chatId}
                onChange={(e) => setChatId(e.target.value)}
              />
              <span className="muted" style={{ fontSize: 12, marginTop: 4 }}>
                Como obter: abra o Telegram, envie <code>/start</code> para{' '}
                <a href="https://t.me/userinfobot" target="_blank" rel="noopener noreferrer">
                  @userinfobot
                </a>{' '}
                e cole o ID numérico aqui.
              </span>
            </div>

            <div className="formField">
              <label htmlFor="username" className="formLabel">
                Username do Telegram (opcional)
              </label>
              <input
                id="username"
                className="input"
                type="text"
                placeholder="Ex: @seunome"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
              />
            </div>

            <div className="formField" style={{ flexDirection: 'row', alignItems: 'center', gap: 10 }}>
              <input
                id="enabled"
                type="checkbox"
                checked={enabled}
                onChange={(e) => setEnabled(e.target.checked)}
                style={{ width: 18, height: 18, cursor: 'pointer' }}
              />
              <label htmlFor="enabled" style={{ cursor: 'pointer' }}>
                Ativar alertas no Telegram
              </label>
            </div>

            {saveMsg && <p className="successMsg">{saveMsg}</p>}
            {error && <p className="errorMsg">{error}</p>}

            <div style={{ display: 'flex', gap: 10, marginTop: 16 }}>
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
                {testing ? 'Enviando...' : 'Testar'}
              </button>
            </div>
          </form>

          {testMsg && (
            <p className="muted" style={{ marginTop: 12 }}>
              {testMsg}
            </p>
          )}
        </section>

        <section className="card" style={{ maxWidth: 560, marginTop: 24 }}>
          <h2 className="cardTitle" style={{ marginBottom: 8 }}>Status atual</h2>
          {config ? (
            <ul className="settingsStatusList">
              <li>
                <span>Chat ID configurado:</span>
                <strong>{config.telegram_chat_id ? config.telegram_chat_id : '—'}</strong>
              </li>
              <li>
                <span>Alertas ativos:</span>
                <strong>{config.telegram_enabled ? 'Sim' : 'Não'}</strong>
              </li>
              <li>
                <span>Token do bot:</span>
                <strong>{config.bot_token_set ? 'Configurado' : 'Não configurado'}</strong>
              </li>
            </ul>
          ) : (
            <p className="muted">Carregando...</p>
          )}
        </section>
      </div>
    </AppNav>
  );
}
