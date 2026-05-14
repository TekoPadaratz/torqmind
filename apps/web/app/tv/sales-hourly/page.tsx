"use client";
import { useEffect, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import { apiGet, setAuthToken } from "../../lib/api";
import { getToken } from "../../lib/auth";
import { loadSession } from "../../lib/session";

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

export default function TVSalesHourlyPage() {
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
      const today = todayStr();
      const res = await apiGet(`/dashboard/series?dt_ini=${today}&dt_fim=${today}`);
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

  const points = data?.points || [];

  return (
    <div style={{ padding: "24px 32px", background: "#0f172a", minHeight: "100vh", color: "#f1f5f9" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
        <h1 style={{ fontSize: 32, fontWeight: 700 }}>⏱️ Vendas por Hora — Hoje</h1>
        <div style={{ color: "#64748b", fontSize: 14 }}>
          Atualização automática a cada 60s
        </div>
      </div>

      {error && <div style={{ color: "#fb7185", marginBottom: 16 }}>{error}</div>}

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))", gap: 12 }}>
        {points.length === 0 && (
          <div style={{ color: "#64748b", textAlign: "center", padding: 40, gridColumn: "1 / -1" }}>
            Nenhum dado disponível ainda.
          </div>
        )}
        {points.map((point: any, idx: number) => (
          <div
            key={point.hour || point.dt || idx}
            style={{
              padding: "16px",
              background: "#1e293b",
              borderRadius: 8,
              textAlign: "center",
            }}
          >
            <div style={{ fontSize: 14, color: "#64748b", marginBottom: 4 }}>
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
