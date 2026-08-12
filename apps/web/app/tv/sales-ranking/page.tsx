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
  const logoUrl =
    typeof session?.branding?.logo_url === "string" && session.branding.logo_url.trim()
      ? session.branding.logo_url.trim()
      : null;

  return (
    <div className="tvSalesRanking">
      <div className="tvSalesRankingHeader">
        <div className="tvSalesRankingBrand">
          {logoUrl ? (
            <img
              className="tvSalesRankingLogo"
              src={logoUrl}
              alt="Logo da empresa"
              loading="lazy"
              decoding="async"
            />
          ) : null}
          <h1 className="tvSalesRankingTitle">Ranking de vendas</h1>
        </div>
        <div className="tvSalesRankingActions">
          <span className="tvSalesRankingUpdated">
            {lastUpdated ? `Atualizado às ${lastUpdated}` : "Atualização automática"}
          </span>
          <button
            type="button"
            className="tvSalesRankingLogout"
            onClick={() => {
              clearAuth();
              router.push("/");
            }}
          >
            Sair
          </button>
        </div>
      </div>

      {error ? <div className="tvSalesRankingError">{error}</div> : null}

      <div className="tvSalesRankingList">
        {sellers.length === 0 ? (
          <div className="tvSalesRankingEmpty">Nenhum dado disponível ainda.</div>
        ) : null}
        {sellers.map((seller: any, idx: number) => (
          <div
            key={seller.id_vendedor || idx}
            className={`tvSalesRankingRow${idx < 3 ? " tvSalesRankingRow--podium" : ""}`}
            style={{
              borderLeftColor:
                idx === 0 ? "#fbbf24" : idx === 1 ? "#94a3b8" : idx === 2 ? "#b45309" : "transparent",
            }}
          >
            <div
              className="tvSalesRankingPos"
              style={{
                color:
                  idx === 0 ? "#fbbf24" : idx === 1 ? "#94a3b8" : idx === 2 ? "#b45309" : "var(--muted)",
              }}
            >
              {idx + 1}º
            </div>
            <div className="tvSalesRankingName">
              {seller.vendedor || seller.nome || `Vendedor ${idx + 1}`}
            </div>
            <div className="tvSalesRankingValue">
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
