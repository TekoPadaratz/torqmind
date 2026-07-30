"use client";

import { useCallback, useEffect, useState } from "react";
import AppNav from "../components/AppNav";
import { api, apiGet, apiPost, setAuthToken } from "../lib/api";
import { getToken, setToken } from "../lib/auth";
import { extractApiError } from "../lib/errors";

type Status = {
  totp_enabled: boolean;
  totp_required: boolean;
  mfa_reset_required: boolean;
  configured: boolean;
};

type SetupData = {
  secret: string;
  otpauth_uri: string;
  qr_svg: string | null;
  issuer: string;
  account: string;
};

const SETUP_TOKEN_KEY = "torqmind.mfa_setup";

export default function SecurityPage() {
  const [status, setStatus] = useState<Status | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [info, setInfo] = useState<string | null>(null);

  // Setup flow
  const [setupData, setSetupData] = useState<SetupData | null>(null);
  const [setupCode, setSetupCode] = useState("");
  const [recoveryCodes, setRecoveryCodes] = useState<string[] | null>(null);
  const [busy, setBusy] = useState(false);

  // Disable flow
  const [disableCode, setDisableCode] = useState("");
  const [showDisable, setShowDisable] = useState(false);

  // Forced enrollment via setup token (from login redirect)
  const [forced, setForced] = useState(false);
  const [setupToken, setSetupToken] = useState<string | null>(null);

  const authHeaders = useCallback(() => {
    if (forced && setupToken) return { headers: { Authorization: `Bearer ${setupToken}` } };
    return undefined;
  }, [forced, setupToken]);

  const loadStatus = useCallback(async () => {
    try {
      const data = await apiGet("/auth/mfa/status");
      setStatus(data);
    } catch (err: any) {
      setError(extractApiError(err, "Não foi possível carregar o status de segurança."));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const isForced = params.get("setup") === "1";
    const stored = typeof window !== "undefined" ? sessionStorage.getItem(SETUP_TOKEN_KEY) : null;
    if (isForced && stored) {
      setForced(true);
      setSetupToken(stored);
      setLoading(false);
      // Forced enrollment: jump straight into setup.
      void startSetup(stored);
      return;
    }
    const t = getToken();
    if (t) setAuthToken(t);
    void loadStatus();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function startSetup(tokenOverride?: string) {
    setError(null);
    setInfo(null);
    setBusy(true);
    try {
      const cfg = tokenOverride
        ? { headers: { Authorization: `Bearer ${tokenOverride}` } }
        : authHeaders();
      const data = await apiPost("/auth/mfa/setup/start", {}, cfg);
      setSetupData(data);
      setRecoveryCodes(null);
      setSetupCode("");
    } catch (err: any) {
      setError(extractApiError(err, "Não foi possível iniciar a configuração do 2FA."));
    } finally {
      setBusy(false);
    }
  }

  async function confirmSetup(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setBusy(true);
    try {
      const data = await apiPost("/auth/mfa/setup/confirm", { code: setupCode.trim() }, authHeaders());
      setRecoveryCodes(data?.recovery_codes || []);
      setSetupData(null);
      setInfo("Autenticação em dois fatores ativada com sucesso.");
      // Forced enrollment returns a full session token → complete login.
      if (forced && data?.login?.access_token) {
        setToken(data.login.access_token);
        sessionStorage.removeItem(SETUP_TOKEN_KEY);
      }
      if (!forced) await loadStatus();
    } catch (err: any) {
      setError(extractApiError(err, "Código inválido. Tente novamente."));
    } finally {
      setBusy(false);
    }
  }

  async function disable2fa(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setBusy(true);
    try {
      await apiPost("/auth/mfa/disable", { code: disableCode.trim() });
      setInfo("Autenticação em dois fatores desativada.");
      setShowDisable(false);
      setDisableCode("");
      await loadStatus();
    } catch (err: any) {
      setError(extractApiError(err, "Código inválido. Não foi possível desativar."));
    } finally {
      setBusy(false);
    }
  }

  function finishForced() {
    sessionStorage.removeItem(SETUP_TOKEN_KEY);
    void apiGet("/auth/me")
      .then((me) => {
        window.location.href = me?.home_path || me?.default_route || "/sales";
      })
      .catch(() => {
        window.location.href = "/sales";
      });
  }

  const enabled = !!status?.totp_enabled;
  const required = !!status?.totp_required;
  const notConfigured = status && !status.configured;

  return (
    <div>
      {!forced ? <AppNav title="Minha Segurança" /> : (
        <div className="nav">
          <div className="brand"><span>🧠</span><span>TorqMind</span><span className="pill">Segurança</span></div>
        </div>
      )}

      <div className="container">
        {forced ? (
          <div className="card col-12" style={{ maxWidth: 720, margin: "24px auto", borderColor: "var(--color-warning)" }}>
            <div className="sectionEyebrow" style={{ color: "var(--color-warning)" }}>Ação obrigatória</div>
            <h1 style={{ marginTop: 4 }}>Configure a autenticação em dois fatores</h1>
            <div className="muted" style={{ marginTop: 8 }}>
              Sua conta exige autenticação em dois fatores. Configure para continuar.
            </div>
          </div>
        ) : null}

        <div className="bi-grid" style={{ maxWidth: 720, margin: "0 auto", gap: 16 }}>
          <div className="card col-12">
            <div className="sectionEyebrow">Autenticação em dois fatores (2FA)</div>
            <h2 style={{ marginTop: 4 }}>Status da sua conta</h2>
            <div className="muted" style={{ marginTop: 8, lineHeight: 1.5 }}>
              Com a autenticação em dois fatores, além da senha, será necessário informar um
              código temporário gerado no seu aplicativo autenticador. Isso protege sua conta
              mesmo que sua senha seja descoberta.
            </div>
            {!forced ? (
              <div style={{ marginTop: 12 }}>
                {loading ? (
                  <span className="muted">Carregando…</span>
                ) : enabled ? (
                  <span className="pill" style={{ background: "rgba(34,197,94,0.12)", color: "#22c55e" }}>
                    2FA ativo
                  </span>
                ) : required ? (
                  <span className="pill" style={{ background: "rgba(245,158,11,0.12)", color: "var(--color-warning)" }}>
                    2FA obrigatório — pendente de configuração
                  </span>
                ) : (
                  <span className="pill" style={{ background: "rgba(148,163,184,0.12)", color: "var(--muted, #94a3b8)" }}>
                    2FA desativado
                  </span>
                )}
                {status?.mfa_reset_required ? (
                  <span className="pill" style={{ marginLeft: 8, background: "rgba(245,158,11,0.12)", color: "var(--color-warning)" }}>
                    Reset solicitado pelo administrador
                  </span>
                ) : null}
              </div>
            ) : null}
          </div>

          {notConfigured ? (
            <div className="card col-12">
              <div className="muted">
                O 2FA ainda não está disponível neste ambiente (chave de criptografia não
                configurada). Procure o administrador da plataforma.
              </div>
            </div>
          ) : null}

          {error ? (
            <div className="card col-12" style={{ borderColor: "var(--color-negative)" }}>
              <div style={{ color: "var(--color-negative)" }}>{error}</div>
            </div>
          ) : null}
          {info ? (
            <div className="card col-12" style={{ borderColor: "#22c55e" }}>
              <div style={{ color: "#22c55e" }}>{info}</div>
            </div>
          ) : null}

          {/* Recovery codes (shown once) */}
          {recoveryCodes ? (
            <div className="card col-12" style={{ borderColor: "var(--color-warning)" }}>
              <h2>Guarde seus códigos de recuperação</h2>
              <div className="muted" style={{ marginTop: 8 }}>
                Estes códigos servem para acessar sua conta caso você perca o aplicativo
                autenticador. Eles aparecem <strong>uma única vez</strong>. Guarde em local seguro.
                Cada código pode ser usado uma vez.
              </div>
              <div
                style={{
                  marginTop: 12,
                  display: "grid",
                  gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
                  gap: 8,
                  fontFamily: "monospace",
                  fontSize: 15,
                }}
              >
                {recoveryCodes.map((c) => (
                  <div key={c} className="card" style={{ padding: "8px 12px", textAlign: "center" }}>{c}</div>
                ))}
              </div>
              {forced ? (
                <button className="btn" style={{ marginTop: 16 }} onClick={finishForced}>
                  Já guardei meus códigos — continuar
                </button>
              ) : null}
            </div>
          ) : null}

          {/* Activation */}
          {!enabled && !recoveryCodes ? (
            <div className="card col-12">
              <h2>Ativar 2FA</h2>
              {!setupData ? (
                <>
                  <div className="muted" style={{ marginTop: 8 }}>
                    Use o Google Authenticator, Microsoft Authenticator ou app compatível.
                  </div>
                  <button className="btn" style={{ marginTop: 12 }} disabled={busy || !!notConfigured} onClick={() => startSetup()}>
                    {busy ? "Gerando…" : "Ativar autenticação em dois fatores"}
                  </button>
                </>
              ) : (
                <form onSubmit={confirmSetup} style={{ marginTop: 8 }}>
                  <div className="muted" style={{ marginBottom: 12 }}>
                    1. Escaneie o QR Code no seu aplicativo autenticador (Google Authenticator,
                    Microsoft Authenticator ou compatível).
                  </div>
                  {setupData.qr_svg ? (
                    <div style={{ display: "flex", justifyContent: "center" }}>
                      {/* eslint-disable-next-line @next/next/no-img-element */}
                      <img
                        src={setupData.qr_svg}
                        alt="QR Code para autenticador"
                        style={{ width: 200, height: 200, background: "#fff", padding: 8, borderRadius: 8 }}
                      />
                    </div>
                  ) : null}
                  <div className="muted" style={{ marginTop: 12 }}>
                    Não consegue escanear? Informe esta chave manualmente no app:
                  </div>
                  <div
                    className="card"
                    style={{ marginTop: 6, padding: "8px 12px", fontFamily: "monospace", letterSpacing: 1, wordBreak: "break-all", textAlign: "center" }}
                  >
                    {setupData.secret}
                  </div>
                  <label className="muted" style={{ display: "block", marginTop: 16 }} htmlFor="setup-code">
                    2. Informe o código de 6 dígitos gerado pelo app
                  </label>
                  <input
                    id="setup-code"
                    className="input"
                    value={setupCode}
                    onChange={(e) => setSetupCode(e.target.value.replace(/[^0-9]/g, ""))}
                    placeholder="000000"
                    inputMode="numeric"
                    autoComplete="one-time-code"
                    maxLength={6}
                    style={{ marginTop: 6 }}
                  />
                  <button className="btn" type="submit" style={{ marginTop: 12 }} disabled={busy || setupCode.trim().length !== 6}>
                    {busy ? "Confirmando…" : "Confirmar e ativar"}
                  </button>
                </form>
              )}
            </div>
          ) : null}

          {/* Deactivation */}
          {enabled && !recoveryCodes && !forced ? (
            <div className="card col-12">
              <h2>Desativar 2FA</h2>
              <div className="muted" style={{ marginTop: 8 }}>
                Desativar reduz a segurança da sua conta. Será necessário informar um código atual
                do seu aplicativo autenticador (ou um código de recuperação).
              </div>
              {!showDisable ? (
                <button className="btn" style={{ marginTop: 12, background: "transparent", border: "1px solid var(--border)" }} onClick={() => setShowDisable(true)}>
                  Desativar 2FA
                </button>
              ) : (
                <form onSubmit={disable2fa} style={{ marginTop: 12 }}>
                  <input
                    className="input"
                    value={disableCode}
                    onChange={(e) => setDisableCode(e.target.value.replace(/[^0-9A-Za-z-]/g, ""))}
                    placeholder="Código de 6 dígitos ou recuperação"
                    inputMode="numeric"
                    autoComplete="one-time-code"
                    maxLength={14}
                  />
                  <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
                    <button className="btn" type="submit" disabled={busy || !disableCode.trim()} style={{ background: "var(--color-negative)" }}>
                      {busy ? "Desativando…" : "Confirmar desativação"}
                    </button>
                    <button type="button" className="btn" style={{ background: "transparent", border: "1px solid var(--border)" }} onClick={() => { setShowDisable(false); setDisableCode(""); }}>
                      Cancelar
                    </button>
                  </div>
                </form>
              )}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
