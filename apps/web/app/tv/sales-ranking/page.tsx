"use client";
import { useEffect, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import { apiGet, setAuthToken } from "../../lib/api";
import { getToken, clearAuth } from "../../lib/auth";
import { loadSession } from "../../lib/session";

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

export default function TVSalesRankingPage() {
  const router = useRouter();
  const [session, setSession] = useState<any>(null);
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

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
      const res = await apiGet(`/bi/tv/sales-ranking`);
      setData(res);
      setError(null);
    } catch (err: any) {
      setError("Erro ao carregar dados");
    }
  }, [session]);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 60_000);
    return () => clearInterval(interval);
  }, [fetchData]);

  if (!session) {
    return <div style={{ padding: 40, textAlign: "center", color: "#94a3b8" }}>Carregando...</div>;
  }

  const sellers = data?.sellers || data?.sellers_ranking || data?.top_sellers || [];

  return (
    <div style={{ padding: "24px 32px", background: "#0f172a", minHeight: "100vh", color: "#f1f5f9" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
        <h1 style={{ fontSize: 32, fontWeight: 700 }}>🏆 Ranking de Vendas — Hoje</h1>
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <span style={{ color: "#64748b", fontSize: 14 }}>Atualização automática a cada 60s</span>
          <button
            onClick={() => { clearAuth(); router.push("/"); }}
            style={{ padding: "6px 16px", background: "#ef4444", color: "#fff", border: "none", borderRadius: 6, cursor: "pointer", fontSize: 14, fontWeight: 600 }}
          >
            Sair
          </button>
        </div>
      </div>

      {error && <div style={{ color: "#fb7185", marginBottom: 16 }}>{error}</div>}

      <div style={{ display: "grid", gap: 8 }}>
        {sellers.length === 0 && (
          <div style={{ color: "#64748b", textAlign: "center", padding: 40 }}>
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
              background: idx < 3 ? "#1e293b" : "#0f172a",
              borderRadius: 8,
              borderLeft: idx === 0 ? "4px solid #fbbf24" : idx === 1 ? "4px solid #94a3b8" : idx === 2 ? "4px solid #b45309" : "4px solid transparent",
            }}
          >
            <div style={{ fontSize: 28, fontWeight: 800, width: 48, textAlign: "center", color: idx === 0 ? "#fbbf24" : idx === 1 ? "#94a3b8" : idx === 2 ? "#b45309" : "#64748b" }}>
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
