"use client";
import { useState } from "react";
import { api, apiGet } from "../lib/api";
import { getToken, setToken } from "../lib/auth";
import { extractApiError } from "../lib/errors";

export default function ChangePasswordPage() {
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [showCurrent, setShowCurrent] = useState(false);
  const [showNew, setShowNew] = useState(false);
  const [showConfirm, setShowConfirm] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    if (newPassword.length < 8) {
      setError("A nova senha deve ter no mínimo 8 caracteres.");
      return;
    }
    if (newPassword !== confirmPassword) {
      setError("As senhas não conferem.");
      return;
    }

    setLoading(true);
    try {
      const res = await api.post("/auth/change-password", {
        current_password: currentPassword,
        new_password: newPassword,
      });

      if (res.data?.access_token) {
        setToken(res.data.access_token);
      }

      // Redirect to the user's permitted home route, not hardcoded /dashboard
      try {
        const me = await apiGet("/auth/me");
        const dest = me?.home_path || me?.default_route || "/dashboard";
        window.location.href = dest;
      } catch {
        window.location.href = "/dashboard";
      }
    } catch (err: any) {
      setError(extractApiError(err, "Falha ao alterar senha"));
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
            <div style={{ position: 'relative' }}>
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
                style={{
                  position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)',
                  background: 'none', border: 'none', cursor: 'pointer', padding: 4,
                  color: 'var(--muted, #94a3b8)', fontSize: 18, lineHeight: 1,
                }}
              >
                {showCurrent ? '🙈' : '👁'}
              </button>
            </div>
            <label className="muted" htmlFor="new-password">
              Nova senha (mín. 8 caracteres)
            </label>
            <div style={{ position: 'relative' }}>
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
                style={{
                  position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)',
                  background: 'none', border: 'none', cursor: 'pointer', padding: 4,
                  color: 'var(--muted, #94a3b8)', fontSize: 18, lineHeight: 1,
                }}
              >
                {showNew ? '🙈' : '👁'}
              </button>
            </div>
            <label className="muted" htmlFor="confirm-password">
              Confirme a nova senha
            </label>
            <div style={{ position: 'relative' }}>
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
                style={{
                  position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)',
                  background: 'none', border: 'none', cursor: 'pointer', padding: 4,
                  color: 'var(--muted, #94a3b8)', fontSize: 18, lineHeight: 1,
                }}
              >
                {showConfirm ? '🙈' : '👁'}
              </button>
            </div>
            {error && (
              <div className="muted" style={{ color: "var(--color-negative)" }}>
                {error}
              </div>
            )}
            <button className="btn" type="submit" disabled={loading}>
              {loading ? "Salvando..." : "Alterar Senha"}
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}
