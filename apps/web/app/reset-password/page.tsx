"use client";
import { useEffect, useState } from "react";
import { api } from "../lib/api";
import { extractApiError } from "../lib/errors";
import { evaluatePassword, isValidPassword } from "../lib/password-policy.mjs";

export default function ResetPasswordPage() {
  const [token, setToken] = useState<string | null>(null);
  const [checking, setChecking] = useState(true);
  const [email, setEmail] = useState<string>("");
  const [tokenValid, setTokenValid] = useState(false);

  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [showNew, setShowNew] = useState(false);
  const [showConfirm, setShowConfirm] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [done, setDone] = useState(false);
  const [mfaRequired, setMfaRequired] = useState(false);
  const [totpCode, setTotpCode] = useState("");

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const t = params.get("token");
    setToken(t);
    if (!t) {
      setChecking(false);
      return;
    }
    api
      .get("/auth/reset-password/validate", { params: { token: t } })
      .then((res) => {
        setTokenValid(true);
        setEmail(res.data?.email || "");
        setMfaRequired(Boolean(res.data?.mfa_required));
      })
      .catch((err) => {
        setError(extractApiError(err, "Link inválido ou expirado. Solicite um novo."));
      })
      .finally(() => setChecking(false));
  }, []);

  const ruleState = evaluatePassword(newPassword);
  const passwordsMatch = newPassword.length > 0 && newPassword === confirmPassword;
  const canSubmit = isValidPassword(newPassword) && passwordsMatch && !loading && (!mfaRequired || totpCode.trim().length >= 6);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    if (!isValidPassword(newPassword)) {
      setError("A nova senha não atende a todos os requisitos.");
      return;
    }
    if (newPassword !== confirmPassword) {
      setError("As senhas não conferem.");
      return;
    }

    setLoading(true);
    try {
      const payload: Record<string, unknown> = { token, new_password: newPassword };
      if (mfaRequired) payload.totp_code = totpCode.trim();
      await api.post("/auth/reset-password", payload);
      setDone(true);
    } catch (err: any) {
      setError(extractApiError(err, "Não foi possível redefinir a senha."));
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

  return (
    <div>
      <div className="nav">
        <div className="brand">
          <span>🧠</span>
          <span>TorqMind</span>
          <span className="pill">BI operacional</span>
        </div>
        <div className="pill">Redefinir senha</div>
      </div>

      <div className="container">
        <div className="card" style={{ maxWidth: 460, margin: "40px auto" }}>
          <h1>Redefinir senha</h1>

          {checking ? (
            <div className="muted" style={{ marginTop: 16, display: "inline-flex", alignItems: "center", gap: 8 }}>
              <span className="loginSpinner" /> Validando link…
            </div>
          ) : done ? (
            <div className="row" style={{ gap: 12, marginTop: 16 }}>
              <div
                className="muted"
                style={{
                  color: "var(--color-positive)",
                  background: "var(--color-positive-bg)",
                  border: "1px solid var(--border)",
                  borderRadius: 12,
                  padding: "12px 14px",
                  lineHeight: 1.5,
                }}
              >
                Senha redefinida com sucesso. Você já pode entrar com a nova senha.
              </div>
              <a className="btn" href="/" style={{ textAlign: "center", textDecoration: "none" }}>
                Ir para o login
              </a>
            </div>
          ) : !tokenValid ? (
            <div className="row" style={{ gap: 12, marginTop: 16 }}>
              <div className="muted" style={{ color: "var(--color-negative)", lineHeight: 1.5 }}>
                {error || "Link inválido ou expirado. Solicite um novo."}
              </div>
              <a className="btn" href="/forgot-password" style={{ textAlign: "center", textDecoration: "none" }}>
                Solicitar novo link
              </a>
            </div>
          ) : (
            <>
              <div className="muted" style={{ marginTop: 8 }}>
                Crie uma nova senha para sua conta.
              </div>
              <div style={{ height: 16 }} />
              <form onSubmit={onSubmit} className="row" style={{ gap: 12 }}>
                <label className="muted" htmlFor="reset-email">
                  E-mail
                </label>
                <input
                  id="reset-email"
                  className="input"
                  type="email"
                  value={email}
                  readOnly
                  autoComplete="username"
                  style={{ opacity: 0.8, cursor: "not-allowed" }}
                />

                <label className="muted" htmlFor="reset-new-password">
                  Nova senha
                </label>
                <div style={{ position: "relative" }}>
                  <input
                    id="reset-new-password"
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

                <ul style={{ listStyle: "none", padding: 0, margin: "4px 0 4px", display: "grid", gap: 4 }}>
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
                      <span aria-hidden style={{ fontSize: 13 }}>{rule.ok ? "✓" : "○"}</span>
                      {rule.label}
                    </li>
                  ))}
                </ul>

                <label className="muted" htmlFor="reset-confirm-password">
                  Confirme a nova senha
                </label>
                <div style={{ position: "relative" }}>
                  <input
                    id="reset-confirm-password"
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
                    <label className="muted" htmlFor="reset-totp">
                      Código do aplicativo autenticador
                    </label>
                    <div className="muted" style={{ fontSize: 12.5 }}>
                      Esta conta tem 2FA ativo. Informe o código de 6 dígitos do seu app autenticador (ou um código de recuperação).
                    </div>
                    <input
                      id="reset-totp"
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
                {!canSubmit && !loading && tokenValid && (
                  <div className="muted" style={{ fontSize: 12.5 }}>
                    Atenda todos os requisitos da senha e confirme igualmente para habilitar o botão.
                  </div>
                )}
                <button
                  className="btn"
                  type="submit"
                  disabled={!canSubmit}
                  title={!canSubmit ? "Complete os requisitos da senha para continuar" : undefined}
                >
                  {loading ? (
                    <span style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
                      <span className="loginSpinner" /> Salvando…
                    </span>
                  ) : (
                    "Redefinir senha"
                  )}
                </button>
              </form>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
