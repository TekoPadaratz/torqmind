"use client";
import { useEffect, useState } from "react";
import { api, setAuthToken } from "./lib/api";
import { clearAuth, getToken, setToken } from "./lib/auth";
import { extractApiError } from "./lib/errors";

export default function LoginPage() {
  const [email, setEmail] = useState("master@torqmind.com");
  const [password, setPassword] = useState("TorqMind@123");
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
      .then(() => {
        window.location.href = "/scope";
      })
      .catch(() => {
        clearAuth();
        setError("Sessao expirada ou invalida. Faça login novamente.");
      })
      .finally(() => {
        setCheckingSession(false);
      });
  }, []);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    try {
      const res = await api.post("/auth/login", { email, password });
      const token = res.data.access_token as string;
      setToken(token);
      setAuthToken(token);
      window.location.href = "/scope";
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
          <span className="pill">Enterprise SaaS</span>
        </div>
        <div className="pill">Login</div>
      </div>

      <div className="container">
        <div className="card" style={{ maxWidth: 460, margin: "40px auto" }}>
          <h1>Entrar</h1>
          <div className="muted">Acesse com segurança (JWT). Multi-tenant e RLS entram por escopo.</div>
          <div style={{ height: 16 }} />
          {checkingSession ? <div className="muted">Validando sessao...</div> : null}
          <form onSubmit={onSubmit} className="row" style={{ gap: 12 }}>
            <input className="input" value={email} onChange={(e) => setEmail(e.target.value)} placeholder="email" />
            <input
              className="input"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="senha"
            />
            {error && (
              <div className="muted" style={{ color: "#fb7185" }}>
                {error}
              </div>
            )}
            <button className="btn" type="submit" disabled={checkingSession}>
              Entrar
            </button>
          </form>
          <div style={{ height: 18 }} />
          <div className="muted">
            Crie usuários com: <code>docker compose exec api python -m app.cli.seed</code>
          </div>
        </div>
      </div>
    </div>
  );
}
