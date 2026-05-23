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
  const [error, setError] = useState<string | null>(null);
  const [checkingSession, setCheckingSession] = useState(true);

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
    try {
      clearAuth();
      const res = await api.post("/auth/login", { identifier, password });
      const token = res.data.access_token as string;
      setToken(token);
      const session = cacheSession(res.data?.session || res.data || null);

      // Force password change redirect
      if (session?.must_change_password) {
        window.location.href = '/change-password';
        return;
      }

      window.location.href = buildCanonicalProductHref(
        res.data?.home_path || "/dashboard",
        session,
        { scopeEpoch: createScopeEpoch() },
      );
    } catch (err: any) {
      setError(extractApiError(err, "Falha no login"));
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
            <input
              className="input"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="senha"
              autoComplete="current-password"
              disabled={checkingSession}
            />
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
          </form>
        </div>
      </div>
    </div>
  );
}
