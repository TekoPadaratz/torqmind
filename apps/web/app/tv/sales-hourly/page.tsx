"use client";
import { useEffect, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import { apiGet, apiPost, setAuthToken } from "../../lib/api";
import { getToken, setToken, clearAuth } from "../../lib/auth";
import { loadSession } from "../../lib/session";

export default function TVSalesHourlyPage() {
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
      const res = await apiGet(`/bi/tv/sales-hourly`);
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

  const points = data?.points || [];

  return (
    <div style={{ padding: "24px 32px", background: "var(--bg)", minHeight: "100vh", color: "var(--text)" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
        <h1 style={{ fontSize: 32, fontWeight: 700 }}>⏱️ Vendas por Hora — Hoje</h1>
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

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))", gap: 12 }}>
        {points.length === 0 && (
          <div style={{ color: "var(--muted)", textAlign: "center", padding: 40, gridColumn: "1 / -1" }}>
            Nenhum dado disponível ainda.
          </div>
        )}
        {points.map((point: any, idx: number) => (
          <div
            key={point.hour || point.dt || idx}
            style={{
              padding: "16px",
              background: "var(--surface-soft)",
              borderRadius: 8,
              textAlign: "center",
            }}
          >
            <div style={{ fontSize: 14, color: "var(--muted)", marginBottom: 4 }}>
              {point.hour || point.label || point.dt || `${idx}h`}
            </div>
            <div style={{ fontSize: 24, fontWeight: 700, color: "#22d3ee" }}>
              {typeof point.total === "number"
                ? point.total.toLocaleString("pt-BR", { style: "currency", currency: "BRL" })
                : typeof point.faturamento === "number"
                  ? point.faturamento.toLocaleString("pt-BR", { style: "currency", currency: "BRL" })
                  : "—"}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
