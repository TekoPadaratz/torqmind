'use client';

import Image from 'next/image';
import { useCallback, useEffect, useId, useMemo, useRef, useState } from 'react';
import { usePathname } from 'next/navigation';

import { apiGet, apiPost } from '../../lib/api';
import { getClaims, getToken, requireAuth } from '../../lib/auth';
import { useScopeQuery } from '../../lib/scope';

type Capability = { intent_id?: string; label?: string; examples?: string[] };
type ClarificationOption = { label?: string; value?: string; documento_masked?: string };
type ChatMessage = {
  role: 'user' | 'assistant';
  text: string;
  clarificationOptions?: ClarificationOption[];
};

function isKioskClaims(claims: any): boolean {
  const role = String(claims?.user_role || claims?.role || '').toLowerCase();
  return role === 'tenant_kiosk';
}

function parseOptionalInt(value: string | null | undefined): number | undefined {
  if (value == null || value === '') return undefined;
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : undefined;
}

export default function IntelligenceHost() {
  const pathname = usePathname() || '';
  const scope = useScopeQuery();
  const titleId = useId();
  const panelRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  const [ready, setReady] = useState(false);
  const [open, setOpen] = useState(false);
  const [caps, setCaps] = useState<Capability[]>([]);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [draft, setDraft] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [enabled, setEnabled] = useState(true);

  const scopePayload = useMemo(() => {
    const idEmpresa =
      parseOptionalInt(scope.id_empresa) ??
      parseOptionalInt(String(getClaims()?.id_empresa ?? '')) ??
      undefined;
    const branchScopeAll = String(scope.branch_scope || '').toLowerCase() === 'all';
    const filiais = (scope.id_filiais || [])
      .map((f) => parseOptionalInt(String(f)))
      .filter((n): n is number => n != null);
    const singleFilial = parseOptionalInt(scope.id_filial);

    const body: {
      id_empresa?: number;
      id_filial?: number;
      id_filiais?: number[];
      branch_scope?: string;
    } = {};
    if (idEmpresa != null) body.id_empresa = idEmpresa;

    if (branchScopeAll || filiais.length > 1) {
      body.branch_scope = 'all';
      if (filiais.length > 0) body.id_filiais = filiais;
    } else if (singleFilial != null) {
      body.id_filial = singleFilial;
    } else if (filiais.length === 1) {
      body.id_filial = filiais[0];
    }
    return body;
  }, [scope.id_empresa, scope.id_filial, scope.id_filiais, scope.branch_scope]);
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

  // Troca de empresa invalida a conversa em memória
  useEffect(() => {
    setConversationId(null);
    setMessages([]);
  }, [scopePayload.id_empresa, scopePayload.id_filiais, scopePayload.branch_scope]);

  const ensureConversation = useCallback(async () => {
    if (conversationId) return conversationId;
    const created = await apiPost('/ai/conversations', {
      title: 'Assistente',
      ...scopePayload,
    });
    const id = String(created?.id || '');
    if (!id) throw new Error('conversation_create_failed');
    setConversationId(id);
    return id;
  }, [conversationId, scopePayload]);

  const toggleOpen = useCallback(() => {
    setOpen((prev) => !prev);
  }, []);

  const close = useCallback(() => {
    setOpen(false);
  }, []);

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
          setError('Assistente indisponível no momento.');
        } else {
          setError('Não foi possível abrir o assistente.');
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
      if (ev.key === 'Escape') close();
    };
    window.addEventListener('keydown', onKey);
    inputRef.current?.focus();
    return () => window.removeEventListener('keydown', onKey);
  }, [open, close]);

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
  }, [open, messages.length, busy]);

  const send = async (text: string) => {
    const cleaned = text.trim();
    if (!cleaned || busy || !enabled) return;
    setBusy(true);
    setError(null);
    setMessages((prev) => [...prev, { role: 'user', text: cleaned }]);
    setDraft('');
    try {
      const id = await ensureConversation();
      const resp = await apiPost(`/ai/conversations/${id}/messages`, {
        text: cleaned,
        ...scopePayload,
      });
      const answerText = String(resp?.answer_text || '').trim();
      const options = Array.isArray(resp?.clarification_options) ? resp.clarification_options : [];
      if (!answerText) {
        setMessages((prev) => [
          ...prev,
          {
            role: 'assistant',
            text: 'Não consegui montar uma resposta agora. Tente reformular a pergunta.',
          },
        ]);
      } else {
        setMessages((prev) => [
          ...prev,
          {
            role: 'assistant',
            text: answerText,
            clarificationOptions: options.length ? options : undefined,
          },
        ]);
      }
    } catch (err: any) {
      const detail = err?.response?.data?.detail;
      const status = err?.response?.status;
      const msg =
        (typeof detail === 'object' && detail?.message) ||
        err?.response?.data?.message ||
        (status === 500
          ? 'Não consegui consultar os dados agora. Tente de novo em instantes ou reformule a pergunta.'
          : 'Não foi possível enviar a mensagem.');
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', text: String(msg) },
      ]);
      setError(null);
    } finally {
      setBusy(false);
    }
  };

  if (!ready) return null;

  const suggestionChips = (caps || [])
    .map((c) => c.label || (c.examples && c.examples[0]) || '')
    .filter(Boolean)
    .slice(0, 4);

  return (
    <>
      <button
        type="button"
        className={`tmIntelFab${open ? ' tmIntelFabOpen' : ''}`}
        aria-label={open ? 'Fechar Assistente TorqMind' : 'Abrir Assistente TorqMind'}
        aria-expanded={open}
        onClick={toggleOpen}
      >
        <Image src="/brand/Logo_Icone.png" alt="" width={28} height={28} priority={false} />
      </button>

      {open ? (
        <div className="tmIntelOverlay" role="presentation" onClick={close}>
          <div
            ref={panelRef}
            className="tmIntelPanel"
            role="dialog"
            aria-modal="true"
            aria-labelledby={titleId}
            onClick={(e) => e.stopPropagation()}
          >
            <header className="tmIntelHeader">
              <div className="tmIntelTitleRow">
                <Image src="/brand/Logo_Icone.png" alt="" width={22} height={22} />
                <h2 id={titleId}>Assistente TorqMind</h2>
              </div>
              <button type="button" className="tmIntelClose" onClick={close} aria-label="Fechar">
                ×
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
              {messages.length === 0 && !busy ? (
                <p className="tmIntelEmpty">Pergunte sobre vendas, clientes, caixa ou metas.</p>
              ) : (
                messages.map((m, idx) => (
                  <div key={`${m.role}-${idx}`} className={m.role === 'user' ? 'tmIntelMsgUser' : 'tmIntelMsgAsst'}>
                    {m.text}
                    {m.role === 'assistant' && m.clarificationOptions?.length ? (
                      <div className="tmIntelClarify">
                        {m.clarificationOptions.slice(0, 5).map((opt) => {
                          const label = String(opt.label || opt.value || '').trim();
                          if (!label) return null;
                          return (
                            <button
                              key={`${label}-${opt.value || idx}`}
                              type="button"
                              className="tmIntelChip tmIntelClarifyChip"
                              onClick={() => send(`Quanto o cliente ${label} está me devendo?`)}
                            >
                              {label}
                            </button>
                          );
                        })}
                      </div>
                    ) : null}
                  </div>
                ))
              )}
              {busy ? (
                <div className="tmIntelMsgAsst tmIntelTyping" role="status" aria-label="Assistente digitando">
                  <span />
                  <span />
                  <span />
                </div>
              ) : null}
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
                placeholder="Escreva sua pergunta…"
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
          width: 52px;
          height: 52px;
          border-radius: 999px;
          border: 1px solid var(--chrome-border, #3a3228);
          background: var(--chrome-face, #1c1814);
          color: var(--chrome-fg, #f3e8d8);
          cursor: pointer;
          box-shadow: 0 10px 28px rgba(0, 0, 0, 0.35);
          display: grid;
          place-items: center;
          padding: 0;
        }
        .tmIntelFab:hover {
          filter: brightness(1.08);
        }
        .tmIntelFabOpen {
          z-index: 80;
        }
        @media (prefers-reduced-motion: no-preference) {
          .tmIntelFab {
            transition: transform 0.15s ease, filter 0.15s ease;
          }
          .tmIntelFab:hover {
            transform: translateY(-1px);
          }
        }
        .tmIntelOverlay {
          position: fixed;
          inset: 0;
          z-index: 70;
          background: transparent;
        }
        .tmIntelPanel {
          position: fixed;
          right: 20px;
          bottom: 84px;
          width: min(380px, calc(100vw - 24px));
          height: min(520px, calc(100vh - 120px));
          background: var(--surface-panel, var(--surface-base, #161310));
          color: var(--chrome-fg, #f3e8d8);
          border: 1px solid var(--chrome-border, #3a3228);
          border-radius: 16px;
          box-shadow: 0 18px 48px rgba(0, 0, 0, 0.45);
          display: flex;
          flex-direction: column;
          padding: 12px;
          gap: 10px;
        }
        .tmIntelHeader {
          display: flex;
          justify-content: space-between;
          gap: 12px;
          align-items: center;
        }
        .tmIntelTitleRow {
          display: flex;
          align-items: center;
          gap: 8px;
          min-width: 0;
        }
        .tmIntelHeader h2 {
          margin: 0;
          font-size: 0.98rem;
          font-weight: 650;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .tmIntelClose {
          border: 1px solid var(--chrome-border, #3a3228);
          background: transparent;
          color: inherit;
          border-radius: 8px;
          width: 32px;
          height: 32px;
          line-height: 1;
          font-size: 1.2rem;
          cursor: pointer;
        }
        .tmIntelChips {
          display: flex;
          flex-wrap: wrap;
          gap: 6px;
        }
        .tmIntelChip {
          border: 1px solid var(--chrome-border, #3a3228);
          background: var(--surface-elevated, #1c1814);
          color: inherit;
          border-radius: 999px;
          padding: 5px 9px;
          font-size: 0.75rem;
          cursor: pointer;
        }
        .tmIntelClarify {
          display: flex;
          flex-wrap: wrap;
          gap: 6px;
          margin-top: 10px;
        }
        .tmIntelClarifyChip {
          text-align: left;
          max-width: 100%;
        }
        .tmIntelMessages {
          flex: 1;
          overflow: auto;
          display: flex;
          flex-direction: column;
          gap: 8px;
          padding-right: 2px;
          min-height: 0;
        }
        .tmIntelEmpty {
          opacity: 0.72;
          font-size: 0.86rem;
          margin: 8px 2px;
        }
        .tmIntelMsgUser,
        .tmIntelMsgAsst {
          max-width: 92%;
          padding: 9px 11px;
          border-radius: 12px;
          white-space: pre-wrap;
          line-height: 1.4;
          font-size: 0.88rem;
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
        .tmIntelTyping {
          display: flex;
          align-items: center;
          gap: 5px;
          min-height: 36px;
          padding: 12px 14px;
        }
        .tmIntelTyping span {
          width: 7px;
          height: 7px;
          border-radius: 999px;
          background: var(--chrome-fg, #f3e8d8);
          opacity: 0.45;
          animation: tmIntelDot 1.2s ease-in-out infinite;
        }
        .tmIntelTyping span:nth-child(2) {
          animation-delay: 0.15s;
        }
        .tmIntelTyping span:nth-child(3) {
          animation-delay: 0.3s;
        }
        @keyframes tmIntelDot {
          0%,
          80%,
          100% {
            transform: translateY(0);
            opacity: 0.35;
          }
          40% {
            transform: translateY(-4px);
            opacity: 0.9;
          }
        }
        @media (prefers-reduced-motion: reduce) {
          .tmIntelTyping span {
            animation: none;
            opacity: 0.6;
          }
        }
        .tmIntelError {
          color: #f2b8b5;
          font-size: 0.82rem;
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
        @media (max-width: 640px) {
          .tmIntelPanel {
            right: 12px;
            left: 12px;
            width: auto;
            bottom: 76px;
            height: min(68vh, 520px);
          }
          .tmIntelFab {
            right: 14px;
            bottom: 14px;
          }
        }
      `}</style>
    </>
  );
}
