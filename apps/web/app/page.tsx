"use client";
import { useEffect, useState } from "react";
import { api, setAuthToken } from "./lib/api";
import { clearAuth, getToken, setToken } from "./lib/auth";
import { extractApiError } from "./lib/errors";
import { LOGIN_IDENTIFIER_LABEL, LOGIN_IDENTIFIER_PLACEHOLDER } from "./lib/login-copy.mjs";
import { LOGIN_FORM_DEFAULTS } from "./lib/login-form-defaults.mjs";
import { buildCanonicalProductHref, createScopeEpoch } from "./lib/product-scope.mjs";
import { cacheSession } from "./lib/session";
import { isConfirmedSessionInvalidation } from "./lib/session-state.mjs";

export default function LoginPage() {
  const [identifier, setIdentifier] = useState<string>(LOGIN_FORM_DEFAULTS.identifier);
  const [password, setPassword] = useState<string>(LOGIN_FORM_DEFAULTS.password);
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [checkingSession, setCheckingSession] = useState(true);
  const [mfaChallenge, setMfaChallenge] = useState<string | null>(null);
  const [mfaCode, setMfaCode] = useState("");
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    const t = getToken();
    if (!t) {
      setCheckingSession(false);
      return;
    }
    setAuthToken(t);
    api
      .get("/auth/me")
      .then((res) => {
        const session = cacheSession(res.data);
        if (session?.must_change_password) {
          window.location.href = '/change-password';
          return;
        }
        window.location.href = buildCanonicalProductHref(
          res.data?.home_path || "/dashboard",
          session,
          { scopeEpoch: createScopeEpoch() },
        );
      })
      .catch((error) => {
        if (isConfirmedSessionInvalidation(error)) {
          clearAuth();
          setError("Sessão expirada ou inválida. Faça login novamente.");
          return;
        }
        setError("Não foi possível validar a sessão agora. Tente novamente em instantes.");
      })
      .finally(() => {
        setCheckingSession(false);
      });
  }, []);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      clearAuth();
      const res = await api.post("/auth/login", { identifier, password });
      // Two-factor: the password step may return a challenge instead of a token.
      if (res.data?.mfa_required && res.data?.mfa_challenge_token) {
        setMfaChallenge(res.data.mfa_challenge_token as string);
        setMfaCode("");
        return;
      }
      // Enforced enrollment: account requires 2FA but has not configured it yet.
      if (res.data?.mfa_setup_required && res.data?.mfa_setup_token) {
        sessionStorage.setItem("torqmind.mfa_setup", res.data.mfa_setup_token as string);
        window.location.href = "/security?setup=1";
        return;
      }
      finishLogin(res.data);
    } catch (err: any) {
      setError(extractApiError(err, "Falha no login"));
    } finally {
      setSubmitting(false);
    }
  }

  function finishLogin(data: any) {
    const token = data?.access_token as string;
    setToken(token);
    const session = cacheSession(data?.session || data || null);
    if (session?.must_change_password) {
      window.location.href = '/change-password';
      return;
    }
    window.location.href = buildCanonicalProductHref(
      data?.home_path || "/dashboard",
      session,
      { scopeEpoch: createScopeEpoch() },
    );
  }

  async function onSubmitMfa(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      const res = await api.post("/auth/mfa/verify", {
        mfa_challenge_token: mfaChallenge,
        code: mfaCode.trim(),
      });
      finishLogin(res.data);
    } catch (err: any) {
      setError(extractApiError(err, "Código inválido"));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div>
      <div className="nav">
        <div className="brand">
          <span>🧠</span>
          <span>TorqMind</span>
          <span className="pill">BI operacional</span>
        </div>
        <div className="pill">Login</div>
      </div>

      <div className="container">
        <div className="card" style={{ maxWidth: 460, margin: "40px auto" }}>
          <h1>Entrar</h1>
          <div className="muted" style={{ marginTop: 8 }}>
            Acesse sua visão consolidada da operação, do caixa, do risco e do financeiro.
          </div>
          <div style={{ height: 16 }} />
          {mfaChallenge ? (
            <form onSubmit={onSubmitMfa} className="row" style={{ gap: 12 }}>
              <label className="muted" htmlFor="mfa-code">
                Código do aplicativo autenticador
              </label>
              <div className="muted" style={{ fontSize: 13 }}>
                Abra seu Google Authenticator, Microsoft Authenticator ou app compatível e informe o código de 6 dígitos.
              </div>
              <input
                id="mfa-code"
                className="input"
                value={mfaCode}
                onChange={(e) => setMfaCode(e.target.value.replace(/[^0-9A-Za-z-]/g, ""))}
                placeholder="000000"
                inputMode="numeric"
                autoComplete="one-time-code"
                autoFocus
                maxLength={14}
                disabled={submitting}
              />
              {error && (
                <div className="muted" style={{ color: "#fb7185" }}>
                  {error}
                </div>
              )}
              <button className="btn" type="submit" disabled={submitting || !mfaCode.trim()}>
                {submitting ? "Verificando…" : "Confirmar código"}
              </button>
              <button
                type="button"
                className="muted"
                onClick={() => { setMfaChallenge(null); setMfaCode(""); setError(null); }}
                style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: 13 }}
              >
                Voltar
              </button>
            </form>
          ) : (
          <form onSubmit={onSubmit} className="row" style={{ gap: 12 }}>
            <label className="muted" htmlFor="login-identifier">
              {LOGIN_IDENTIFIER_LABEL}
            </label>
            <input
              id="login-identifier"
              className="input"
              value={identifier}
              onChange={(e) => setIdentifier(e.target.value)}
              placeholder={LOGIN_IDENTIFIER_PLACEHOLDER}
              autoComplete="username"
              autoCapitalize="none"
              autoCorrect="off"
              spellCheck={false}
              disabled={checkingSession}
            />
            <div style={{ position: 'relative' }}>
              <input
                className="input"
                type={showPassword ? "text" : "password"}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="senha"
                autoComplete="current-password"
                disabled={checkingSession}
                style={{ paddingRight: 40 }}
              />
              <button
                type="button"
                onClick={() => setShowPassword(!showPassword)}
                aria-label={showPassword ? "Ocultar senha" : "Mostrar senha"}
                style={{
                  position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)',
                  background: 'none', border: 'none', cursor: 'pointer', padding: 4,
                  color: 'var(--muted, #94a3b8)', fontSize: 18, lineHeight: 1,
                }}
              >
                {showPassword ? '🙈' : '👁'}
              </button>
            </div>
            {error && (
              <div className="muted" style={{ color: "#fb7185" }}>
                {error}
              </div>
            )}
            <button className="btn" type="submit" disabled={checkingSession}>
              {checkingSession ? (
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                  <span className="loginSpinner" /> Verificando sessão…
                </span>
              ) : 'Entrar'}
            </button>
            <a
              href="/forgot-password"
              className="muted"
              style={{ textAlign: 'center', textDecoration: 'none', fontSize: 13 }}
            >
              Esqueci minha senha
            </a>
          </form>
          )}
        </div>
      </div>
    </div>
  );
}
