"use client";
import { useEffect, useState, useCallback, useMemo } from "react";
import { useRouter } from "next/navigation";
import { apiGet, apiPost, setAuthToken } from "../../lib/api";
import { getToken, setToken, clearAuth } from "../../lib/auth";
import { loadSession } from "../../lib/session";
import SalesFloorBoard, { SalesFloorHourPoint } from "../../components/SalesFloorBoard";

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
      try {
        const refreshRes = await apiPost("/auth/refresh", {});
        if (refreshRes?.access_token) {
          setToken(refreshRes.access_token);
        }
      } catch {}
      const res = await apiGet(`/bi/tv/sales-hourly`);
      setData(res);
      setLastUpdated(
        new Date().toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" }),
      );
      setError(null);
    } catch {
      setError("Erro ao carregar dados");
    }
  }, [session]);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 300_000);
    return () => clearInterval(interval);
  }, [fetchData]);

  const hours: SalesFloorHourPoint[] = useMemo(() => {
    const rows = new Array(24).fill(0).map((_, hora) => ({
      hora: `${hora.toString().padStart(2, "0")}:00`,
      saidas: 0,
    }));
    for (const point of data?.points || []) {
      const hour = Number(
        point?.hora ?? String(point?.hour || point?.label || "").slice(0, 2),
      );
      if (hour >= 0 && hour < 24) {
        rows[hour].saidas += Number(
          point?.total ?? point?.faturamento ?? point?.saidas ?? 0,
        );
      }
    }
    return rows;
  }, [data]);

  if (!session) {
    return (
      <div style={{ padding: 40, textAlign: "center", color: "var(--muted)" }}>
        Carregando...
      </div>
    );
  }

  const totals = data?.totals || {};

  return (
    <SalesFloorBoard
      title="Vendas por hora — Hoje"
      subtitle="Totalizadores do dia e distribuição horária"
      lastUpdated={lastUpdated}
      totals={{
        vendas: Number(totals.vendas || 0),
        qtd_vendas: Number(totals.qtd_vendas || 0),
        cancelamentos: Number(totals.cancelamentos || 0),
        qtd_cancelamentos: Number(totals.qtd_cancelamentos || 0),
        devolucoes: Number(totals.devolucoes || 0),
        qtd_devolucoes: Number(totals.qtd_devolucoes || 0),
      }}
      hours={hours}
      error={error}
      showLogout
      onLogout={() => {
        clearAuth();
        router.push("/");
      }}
    />
  );
}
