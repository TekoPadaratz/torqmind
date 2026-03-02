"use client";
import { useEffect, useState } from "react";
import { api, setAuthToken } from "./lib/api";
import { getToken, setToken } from "./lib/auth";

function formatApiError(err: any): string {
  // PT-BR: FastAPI 422 retorna detail = [{type, loc, msg, ...}]
  // EN: FastAPI 422 returns detail = [{type, loc, msg, ...}]
  const detail = err?.response?.data?.detail;

  if (!detail) return "Falha no login";
  if (typeof detail === "string") return detail;

  if (Array.isArray(detail)) {
    const msgs = detail
      .map((d) => (typeof d?.msg === "string" ? d.msg : JSON.stringify(d)))
      .filter(Boolean);
    return msgs.length ? msgs.join("; ") : "Falha no login";
  }

  if (typeof detail === "object") {
    if (typeof detail.msg === "string") return detail.msg;
    return JSON.stringify(detail);
  }

  return String(detail);
}

export default function LoginPage() {
  const [email, setEmail] = useState("master@torqmind.com");
  const [password, setPassword] = useState("TorqMind@123");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const t = getToken();
    if (t) {
      setAuthToken(t);
      window.location.href = "/scope";
    }
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
      setError(formatApiError(err));
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
            <button className="btn" type="submit">
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
