"use client";
import { useEffect, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import { apiGet, apiPost, setAuthToken } from "../../lib/api";
import { getToken, setToken, clearAuth } from "../../lib/auth";
import { loadSession } from "../../lib/session";

export default function TVSalesRankingPage() {
  const router = useRouter();
  const [session, setSession] = useState<any>(null);
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);

  useEffect(() => {
    const t = getToken();
    if (t) setAuthToken(t);
    loadSession(router, "product").then((me) => {
      if (me) setSession(me);
    });
  }, [router]);

  const fetchData = useCallback(async () => {
    if (!session) return;
    try {
      // Refresh token to keep kiosk session alive
      try {
        const refreshRes = await apiPost("/auth/refresh", {});
        if (refreshRes?.access_token) {
          setToken(refreshRes.access_token);
        }
      } catch {}
      const res = await apiGet(`/bi/tv/sales-ranking`);
      setData(res);
      setLastUpdated(new Date().toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" }));
      setError(null);
    } catch (err: any) {
      setError("Erro ao carregar dados");
    }
  }, [session]);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 300_000);
    return () => clearInterval(interval);
  }, [fetchData]);

  if (!session) {
    return <div style={{ padding: 40, textAlign: "center", color: "var(--muted)" }}>Carregando...</div>;
  }

  const sellers = data?.sellers || data?.sellers_ranking || data?.top_sellers || [];

  return (
    <div style={{ padding: "24px 32px", background: "var(--bg)", minHeight: "100vh", color: "var(--text)" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
        <h1 style={{ fontSize: 32, fontWeight: 700 }}>🏆 Ranking de Vendas — Hoje</h1>
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <span style={{ color: "var(--muted)", fontSize: 14 }}>
            {lastUpdated ? `Última atualização: ${lastUpdated}` : "Atualização automática a cada 5 min"}
          </span>
          <button
            onClick={() => { clearAuth(); router.push("/"); }}
            style={{ padding: "6px 16px", background: "var(--color-negative)", color: "var(--on-accent)", border: "none", borderRadius: 6, cursor: "pointer", fontSize: 14, fontWeight: 600 }}
          >
            Sair
          </button>
        </div>
      </div>

      {error && <div style={{ color: "var(--color-negative)", marginBottom: 16 }}>{error}</div>}

      <div style={{ display: "grid", gap: 8 }}>
        {sellers.length === 0 && (
          <div style={{ color: "var(--muted)", textAlign: "center", padding: 40 }}>
            Nenhum dado disponível ainda.
          </div>
        )}
        {sellers.map((seller: any, idx: number) => (
          <div
            key={seller.id_vendedor || idx}
            style={{
              display: "flex",
              alignItems: "center",
              gap: 16,
              padding: "16px 20px",
              background: idx < 3 ? "var(--surface-soft)" : "var(--bg)",
              borderRadius: 8,
              borderLeft: idx === 0 ? "4px solid #fbbf24" : idx === 1 ? "4px solid #94a3b8" : idx === 2 ? "4px solid #b45309" : "4px solid transparent",
            }}
          >
            <div style={{ fontSize: 28, fontWeight: 800, width: 48, textAlign: "center", color: idx === 0 ? "#fbbf24" : idx === 1 ? "#94a3b8" : idx === 2 ? "#b45309" : "var(--muted)" }}>
              {idx + 1}º
            </div>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 18, fontWeight: 600 }}>{seller.vendedor || seller.nome || `Vendedor ${idx + 1}`}</div>
            </div>
            <div style={{ fontSize: 22, fontWeight: 700, color: "#22d3ee" }}>
              {typeof seller.total === "number"
                ? seller.total.toLocaleString("pt-BR", { style: "currency", currency: "BRL" })
                : seller.total ?? "—"}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
