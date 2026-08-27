'use client';

import { useCallback, useEffect, useId, useRef, useState } from 'react';
import { usePathname } from 'next/navigation';

import { apiGet, apiPost } from '../../lib/api';
import { getClaims, getToken, requireAuth } from '../../lib/auth';

type Capability = { intent_id?: string; label?: string; examples?: string[] };

const DISCLAIMER =
  'Respostas baseadas nos dados do TorqMind. O assistente não altera informações.';

function isKioskClaims(claims: any): boolean {
  const role = String(claims?.user_role || claims?.role || '').toLowerCase();
  return role === 'tenant_kiosk';
}

export default function IntelligenceHost() {
  const pathname = usePathname() || '';
  const titleId = useId();
  const panelRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  const [ready, setReady] = useState(false);
  const [open, setOpen] = useState(false);
  const [caps, setCaps] = useState<Capability[]>([]);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [messages, setMessages] = useState<{ role: string; text: string }[]>([]);
  const [draft, setDraft] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [enabled, setEnabled] = useState(true);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (pathname === '/' || pathname.startsWith('/tv') || pathname.startsWith('/login')) {
      setReady(false);
      return;
    }
    if (!requireAuth()) {
      setReady(false);
      return;
    }
    const claims = getClaims();
    if (!getToken() || isKioskClaims(claims)) {
      setReady(false);
      return;
    }
    setReady(true);
  }, [pathname]);

  const ensureConversation = useCallback(async () => {
    if (conversationId) return conversationId;
    const created = await apiPost('/ai/conversations', { title: 'Assistente' });
    const id = String(created?.id || '');
    setConversationId(id);
    return id;
  }, [conversationId]);

  useEffect(() => {
    if (!ready || !open) return;
    let cancelled = false;
    (async () => {
      try {
        const data = await apiGet('/ai/capabilities');
        if (cancelled) return;
        setCaps(Array.isArray(data?.items) ? data.items : []);
        setEnabled(true);
        setError(null);
      } catch (err: any) {
        const status = err?.response?.status;
        if (status === 503) {
          setEnabled(false);
          setError('Assistente desligado neste ambiente.');
        } else {
          setError('Não foi possível carregar o assistente.');
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [ready, open]);

  useEffect(() => {
    if (!open) return;
    const onKey = (ev: KeyboardEvent) => {
      if (ev.key === 'Escape') setOpen(false);
    };
    window.addEventListener('keydown', onKey);
    inputRef.current?.focus();
    return () => window.removeEventListener('keydown', onKey);
  }, [open]);

  useEffect(() => {
    if (!open || !panelRef.current) return;
    const root = panelRef.current;
    const focusables = root.querySelectorAll<HTMLElement>(
      'button, [href], input, textarea, select, [tabindex]:not([tabindex="-1"])'
    );
    const first = focusables[0];
    const last = focusables[focusables.length - 1];
    const trap = (ev: KeyboardEvent) => {
      if (ev.key !== 'Tab' || focusables.length === 0) return;
      if (ev.shiftKey && document.activeElement === first) {
        ev.preventDefault();
        last?.focus();
      } else if (!ev.shiftKey && document.activeElement === last) {
        ev.preventDefault();
        first?.focus();
      }
    };
    root.addEventListener('keydown', trap);
    return () => root.removeEventListener('keydown', trap);
  }, [open, messages.length]);

  const send = async (text: string) => {
    const cleaned = text.trim();
    if (!cleaned || busy || !enabled) return;
    setBusy(true);
    setError(null);
    setMessages((prev) => [...prev, { role: 'user', text: cleaned }]);
    setDraft('');
    try {
      const id = await ensureConversation();
      const resp = await apiPost(`/ai/conversations/${id}/messages`, { text: cleaned });
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', text: String(resp?.answer_text || 'Sem resposta.') },
      ]);
    } catch (err: any) {
      const msg =
        err?.response?.data?.detail?.message ||
        err?.response?.data?.message ||
        'Falha ao enviar a mensagem.';
      setError(String(msg));
    } finally {
      setBusy(false);
    }
  };

  if (!ready) return null;

  const suggestionChips = (caps || [])
    .map((c) => c.label || (c.examples && c.examples[0]) || '')
    .filter(Boolean)
    .slice(0, 6);

  return (
    <>
      <button
        type="button"
        className="tmIntelFab"
        aria-label="Abrir Assistente TorqMind"
        onClick={() => setOpen(true)}
      >
        ?
      </button>

      {open ? (
        <div className="tmIntelOverlay" role="presentation" onClick={() => setOpen(false)}>
          <div
            ref={panelRef}
            className="tmIntelDrawer"
            role="dialog"
            aria-modal="true"
            aria-labelledby={titleId}
            onClick={(e) => e.stopPropagation()}
          >
            <header className="tmIntelHeader">
              <div>
                <h2 id={titleId}>Assistente TorqMind</h2>
                <p className="tmIntelDisclaimer">{DISCLAIMER}</p>
              </div>
              <button type="button" className="tmIntelClose" onClick={() => setOpen(false)} aria-label="Fechar">
                Esc
              </button>
            </header>

            <div className="tmIntelChips">
              <button type="button" className="tmIntelChip" onClick={() => send('O que posso perguntar?')}>
                O que posso perguntar?
              </button>
              {suggestionChips.map((chip) => (
                <button key={chip} type="button" className="tmIntelChip" onClick={() => send(String(chip))}>
                  {chip}
                </button>
              ))}
            </div>

            <div className="tmIntelMessages" aria-live="polite">
              {messages.length === 0 ? (
                <p className="tmIntelEmpty">Pergunte sobre vendas, clientes, caixa, metas…</p>
              ) : (
                messages.map((m, idx) => (
                  <div key={`${m.role}-${idx}`} className={m.role === 'user' ? 'tmIntelMsgUser' : 'tmIntelMsgAsst'}>
                    {m.text}
                  </div>
                ))
              )}
            </div>

            {error ? <div className="tmIntelError">{error}</div> : null}

            <form
              className="tmIntelComposer"
              onSubmit={(e) => {
                e.preventDefault();
                void send(draft);
              }}
            >
              <input
                ref={inputRef}
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
                placeholder="Pergunte aos dados…"
                disabled={busy || !enabled}
                maxLength={2000}
              />
              <button type="submit" disabled={busy || !enabled || !draft.trim()}>
                Enviar
              </button>
            </form>
          </div>
        </div>
      ) : null}

      <style jsx global>{`
        .tmIntelFab {
          position: fixed;
          right: 20px;
          bottom: 20px;
          z-index: 60;
          width: 48px;
          height: 48px;
          border-radius: 999px;
          border: 1px solid var(--chrome-border, #3a3228);
          background: var(--surface-elevated, #1c1814);
          color: var(--chrome-fg, #f3e8d8);
          font-size: 1.25rem;
          font-weight: 700;
          cursor: pointer;
          box-shadow: 0 8px 24px rgba(0, 0, 0, 0.35);
        }
        .tmIntelOverlay {
          position: fixed;
          inset: 0;
          z-index: 70;
          background: rgba(0, 0, 0, 0.45);
          display: flex;
          justify-content: flex-end;
        }
        .tmIntelDrawer {
          width: min(420px, 100vw);
          height: 100%;
          background: var(--surface-base, #12100e);
          color: var(--chrome-fg, #f3e8d8);
          border-left: 1px solid var(--chrome-border, #3a3228);
          display: flex;
          flex-direction: column;
          padding: 16px;
          gap: 12px;
        }
        .tmIntelHeader {
          display: flex;
          justify-content: space-between;
          gap: 12px;
          align-items: flex-start;
        }
        .tmIntelHeader h2 {
          margin: 0;
          font-size: 1.1rem;
        }
        .tmIntelDisclaimer {
          margin: 6px 0 0;
          font-size: 0.8rem;
          opacity: 0.8;
          line-height: 1.35;
        }
        .tmIntelClose {
          border: 1px solid var(--chrome-border, #3a3228);
          background: transparent;
          color: inherit;
          border-radius: 8px;
          padding: 6px 10px;
          cursor: pointer;
        }
        .tmIntelChips {
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
        }
        .tmIntelChip {
          border: 1px solid var(--chrome-border, #3a3228);
          background: var(--surface-elevated, #1c1814);
          color: inherit;
          border-radius: 999px;
          padding: 6px 10px;
          font-size: 0.8rem;
          cursor: pointer;
        }
        .tmIntelMessages {
          flex: 1;
          overflow: auto;
          display: flex;
          flex-direction: column;
          gap: 10px;
          padding-right: 4px;
        }
        .tmIntelEmpty {
          opacity: 0.7;
          font-size: 0.9rem;
        }
        .tmIntelMsgUser,
        .tmIntelMsgAsst {
          max-width: 92%;
          padding: 10px 12px;
          border-radius: 12px;
          white-space: pre-wrap;
          line-height: 1.4;
          font-size: 0.92rem;
        }
        .tmIntelMsgUser {
          align-self: flex-end;
          background: #3a2a18;
        }
        .tmIntelMsgAsst {
          align-self: flex-start;
          background: var(--surface-elevated, #1c1814);
          border: 1px solid var(--chrome-border, #3a3228);
        }
        .tmIntelError {
          color: #f2b8b5;
          font-size: 0.85rem;
        }
        .tmIntelComposer {
          display: flex;
          gap: 8px;
        }
        .tmIntelComposer input {
          flex: 1;
          border-radius: 10px;
          border: 1px solid var(--chrome-border, #3a3228);
          background: var(--surface-elevated, #1c1814);
          color: inherit;
          padding: 10px 12px;
        }
        .tmIntelComposer button {
          border-radius: 10px;
          border: 1px solid var(--chrome-border, #3a3228);
          background: #8a5a2b;
          color: #fff8ef;
          padding: 10px 14px;
          cursor: pointer;
        }
        .tmIntelComposer button:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }
      `}</style>
    </>
  );
}
