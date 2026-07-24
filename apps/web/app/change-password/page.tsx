"use client";

import { useEffect, useState } from "react";
import { api, apiGet, setAuthToken } from "../lib/api";
import { clearAuth, getToken, requireAuth, setToken } from "../lib/auth";
import { extractApiError } from "../lib/errors";
import { evaluatePassword, isValidPassword } from "../lib/password-policy.mjs";

export default function ChangePasswordPage() {
  const [ready, setReady] = useState(false);
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [showCurrent, setShowCurrent] = useState(false);
  const [showNew, setShowNew] = useState(false);
  const [showConfirm, setShowConfirm] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [mfaRequired, setMfaRequired] = useState(false);
  const [totpCode, setTotpCode] = useState("");

  useEffect(() => {
    // Após redirect full-page do login, o axios perde o Authorization no default.
    // Sem isto o POST /auth/change-password volta 401 e o interceptor manda ao login.
    if (!requireAuth()) {
      window.location.href = "/";
      return;
    }
    const token = getToken();
    if (token) setAuthToken(token);

    apiGet("/auth/me")
      .catch((err) => {
        const status = err?.response?.status;
        if (status === 401) {
          clearAuth();
          window.location.href = "/";
        }
      })
      .finally(() => setReady(true));

    // Descobre 2FA sem bloquear a tela se o endpoint falhar.
    api
      .get("/auth/mfa/status")
      .then((res) => {
        setMfaRequired(Boolean(res.data?.totp_enabled || res.data?.enabled));
      })
      .catch(() => {
        /* opcional */
      });
  }, []);

  const ruleState = evaluatePassword(newPassword);
  const passwordsMatch = newPassword.length > 0 && newPassword === confirmPassword;
  const canSubmit =
    ready &&
    currentPassword.length > 0 &&
    isValidPassword(newPassword) &&
    passwordsMatch &&
    !loading &&
    (!mfaRequired || totpCode.trim().length >= 6);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    if (!requireAuth()) {
      window.location.href = "/";
      return;
    }
    const token = getToken();
    if (token) setAuthToken(token);

    if (!isValidPassword(newPassword)) {
      setError("A nova senha não atende a todos os requisitos.");
      return;
    }
    if (newPassword !== confirmPassword) {
      setError("As senhas não conferem.");
      return;
    }
    if (currentPassword === newPassword) {
      setError("A nova senha deve ser diferente da senha atual.");
      return;
    }

    setLoading(true);
    try {
      const payload: Record<string, unknown> = {
        current_password: currentPassword,
        new_password: newPassword,
      };
      if (mfaRequired) payload.totp_code = totpCode.trim();

      const res = await api.post("/auth/change-password", payload);
      if (res.data?.access_token) {
        setToken(res.data.access_token);
      }

      try {
        const me = await apiGet("/auth/me");
        const dest = me?.home_path || me?.default_route || "/dashboard";
        window.location.href = dest;
      } catch {
        window.location.href = "/dashboard";
      }
    } catch (err: any) {
      const status = err?.response?.status;
      const code = err?.response?.data?.error || err?.response?.data?.detail?.error;
      if (status === 401 && code === "mfa_required") {
        setMfaRequired(true);
        setError(extractApiError(err, "Informe o código do autenticador."));
      } else {
        setError(extractApiError(err, "Falha ao alterar senha"));
      }
    } finally {
      setLoading(false);
    }
  }

  const eyeButtonStyle: React.CSSProperties = {
    position: "absolute",
    right: 10,
    top: "50%",
    transform: "translateY(-50%)",
    background: "none",
    border: "none",
    cursor: "pointer",
    padding: 4,
    color: "var(--muted, #94a3b8)",
    fontSize: 18,
    lineHeight: 1,
  };

  if (!ready) {
    return (
      <div className="container">
        <div className="card" style={{ maxWidth: 460, margin: "40px auto" }}>
          <div className="muted" style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
            <span className="loginSpinner" /> Carregando…
          </div>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="nav">
        <div className="brand">
          <span>🧠</span>
          <span>TorqMind</span>
          <span className="pill">Plataforma Operacional</span>
        </div>
        <div className="pill">Alterar Senha</div>
      </div>

      <div className="container">
        <div className="card" style={{ maxWidth: 460, margin: "40px auto" }}>
          <h1>Alterar Senha</h1>
          <div className="muted" style={{ marginTop: 8 }}>
            Você precisa alterar sua senha antes de continuar usando o sistema.
          </div>
          <div style={{ height: 16 }} />
          <form onSubmit={onSubmit} className="row" style={{ gap: 12 }}>
            <label className="muted" htmlFor="current-password">
              Senha atual
            </label>
            <div style={{ position: "relative" }}>
              <input
                id="current-password"
                className="input"
                type={showCurrent ? "text" : "password"}
                value={currentPassword}
                onChange={(e) => setCurrentPassword(e.target.value)}
                placeholder="Senha atual"
                autoComplete="current-password"
                style={{ paddingRight: 40 }}
              />
              <button
                type="button"
                onClick={() => setShowCurrent(!showCurrent)}
                aria-label={showCurrent ? "Ocultar senha" : "Mostrar senha"}
                style={eyeButtonStyle}
              >
                {showCurrent ? "🙈" : "👁"}
              </button>
            </div>

            <label className="muted" htmlFor="new-password">
              Nova senha
            </label>
            <div style={{ position: "relative" }}>
              <input
                id="new-password"
                className="input"
                type={showNew ? "text" : "password"}
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
                placeholder="Nova senha"
                autoComplete="new-password"
                style={{ paddingRight: 40 }}
              />
              <button
                type="button"
                onClick={() => setShowNew(!showNew)}
                aria-label={showNew ? "Ocultar senha" : "Mostrar senha"}
                style={eyeButtonStyle}
              >
                {showNew ? "🙈" : "👁"}
              </button>
            </div>

            <ul style={{ listStyle: "none", padding: 0, margin: "4px 0", display: "grid", gap: 4 }}>
              {ruleState.map((rule) => (
                <li
                  key={rule.key}
                  style={{
                    fontSize: 12.5,
                    display: "flex",
                    alignItems: "center",
                    gap: 8,
                    color: rule.ok ? "var(--color-positive)" : "var(--muted, #94a3b8)",
                  }}
                >
                  <span aria-hidden style={{ fontSize: 13 }}>
                    {rule.ok ? "✓" : "○"}
                  </span>
                  {rule.label}
                </li>
              ))}
            </ul>

            <label className="muted" htmlFor="confirm-password">
              Confirme a nova senha
            </label>
            <div style={{ position: "relative" }}>
              <input
                id="confirm-password"
                className="input"
                type={showConfirm ? "text" : "password"}
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
                placeholder="Confirme a nova senha"
                autoComplete="new-password"
                style={{ paddingRight: 40 }}
              />
              <button
                type="button"
                onClick={() => setShowConfirm(!showConfirm)}
                aria-label={showConfirm ? "Ocultar senha" : "Mostrar senha"}
                style={eyeButtonStyle}
              >
                {showConfirm ? "🙈" : "👁"}
              </button>
            </div>
            {confirmPassword.length > 0 && !passwordsMatch && (
              <div className="muted" style={{ color: "var(--color-warning)", fontSize: 12.5 }}>
                As senhas não conferem.
              </div>
            )}

            {mfaRequired && (
              <>
                <label className="muted" htmlFor="change-totp">
                  Código do aplicativo autenticador
                </label>
                <input
                  id="change-totp"
                  className="input"
                  value={totpCode}
                  onChange={(e) => setTotpCode(e.target.value.replace(/[^0-9A-Za-z-]/g, ""))}
                  placeholder="000000"
                  inputMode="numeric"
                  autoComplete="one-time-code"
                  maxLength={14}
                />
              </>
            )}

            {error && (
              <div className="muted" style={{ color: "var(--color-negative)" }}>
                {error}
              </div>
            )}

            {!canSubmit && !loading && (
              <div className="muted" style={{ fontSize: 12.5 }}>
                Preencha a senha atual, atenda todos os requisitos e confirme a nova senha para habilitar o botão.
              </div>
            )}

            <button className="btn" type="submit" disabled={!canSubmit}>
              {loading ? "Salvando..." : "Alterar Senha"}
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}
