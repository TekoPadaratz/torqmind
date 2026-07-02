"use client";
import { useState } from "react";
import { api } from "../lib/api";
import { extractApiError } from "../lib/errors";

export default function ForgotPasswordPage() {
  const [identifier, setIdentifier] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [done, setDone] = useState(false);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setMessage(null);

    if (!identifier.trim()) {
      setError("Informe o e-mail cadastrado.");
      return;
    }

    setLoading(true);
    try {
      const res = await api.post("/auth/forgot-password", { identifier: identifier.trim() });
      setMessage(
        res.data?.message ||
          "Se houver uma conta para este e-mail, enviaremos um link de recuperação em instantes.",
      );
      setDone(true);
    } catch (err: any) {
      setError(extractApiError(err, "Não foi possível processar a solicitação. Tente novamente."));
    } finally {
      setLoading(false);
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
        <div className="pill">Recuperar senha</div>
      </div>

      <div className="container">
        <div className="card" style={{ maxWidth: 460, margin: "40px auto" }}>
          <h1>Esqueci minha senha</h1>
          <div className="muted" style={{ marginTop: 8 }}>
            Informe o e-mail cadastrado e enviaremos um link seguro para você criar uma nova senha.
          </div>
          <div style={{ height: 16 }} />

          {done ? (
            <div className="row" style={{ gap: 12 }}>
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
                {message}
              </div>
              <a className="btn" href="/" style={{ textAlign: "center", textDecoration: "none" }}>
                Voltar ao login
              </a>
            </div>
          ) : (
            <form onSubmit={onSubmit} className="row" style={{ gap: 12 }}>
              <label className="muted" htmlFor="forgot-email">
                E-mail cadastrado
              </label>
              <input
                id="forgot-email"
                className="input"
                type="email"
                value={identifier}
                onChange={(e) => setIdentifier(e.target.value)}
                placeholder="voce@empresa.com"
                autoComplete="email"
                autoCapitalize="none"
                autoCorrect="off"
                spellCheck={false}
                inputMode="email"
                disabled={loading}
              />
              {error && (
                <div className="muted" style={{ color: "var(--color-negative)" }}>
                  {error}
                </div>
              )}
              <button className="btn" type="submit" disabled={loading}>
                {loading ? (
                  <span style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
                    <span className="loginSpinner" /> Enviando…
                  </span>
                ) : (
                  "Recuperar senha"
                )}
              </button>
              <a
                href="/"
                className="muted"
                style={{ textAlign: "center", textDecoration: "none", fontSize: 13 }}
              >
                Voltar ao login
              </a>
            </form>
          )}
        </div>
      </div>
    </div>
  );
}
