"use client";
import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { setAuthToken } from "../lib/api";
import { getToken } from "../lib/auth";
import { loadSession } from "../lib/session";
import Link from "next/link";

export default function TVIndexPage() {
  const router = useRouter();
  const [session, setSession] = useState<any>(null);

  useEffect(() => {
    const t = getToken();
    if (t) setAuthToken(t);
    loadSession(router, "product").then((me) => {
      if (me) setSession(me);
    });
  }, [router]);

  if (!session) {
    return <div style={{ padding: 40, textAlign: "center", color: "#94a3b8" }}>Carregando...</div>;
  }

  return (
    <div style={{ padding: 40, maxWidth: 600, margin: "0 auto" }}>
      <h1 style={{ fontSize: 28, marginBottom: 24 }}>📺 Modo TV</h1>
      <p style={{ color: "#94a3b8", marginBottom: 32 }}>
        Selecione a tela para exibição no ponto de venda:
      </p>
      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        <Link
          href="/tv/sales-ranking"
          style={{
            display: "block",
            padding: "20px 24px",
            background: "#1e293b",
            borderRadius: 12,
            color: "#f1f5f9",
            textDecoration: "none",
            fontSize: 18,
          }}
        >
          🏆 Ranking de Vendas
        </Link>
        <Link
          href="/tv/sales-hourly"
          style={{
            display: "block",
            padding: "20px 24px",
            background: "#1e293b",
            borderRadius: 12,
            color: "#f1f5f9",
            textDecoration: "none",
            fontSize: 18,
          }}
        >
          ⏱️ Vendas por Hora
        </Link>
      </div>
    </div>
  );
}
