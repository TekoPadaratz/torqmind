"use client";
import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { setAuthToken } from "../lib/api";
import { getToken, clearAuth } from "../lib/auth";
import { loadSession } from "../lib/session";
import Link from "next/link";

const TV_LINKS = [
  { screen_key: "tv_sales_ranking", href: "/tv/sales-ranking", label: "🏆 Ranking de Vendas" },
  { screen_key: "tv_sales_hourly", href: "/tv/sales-hourly", label: "⏱️ Vendas por Hora" },
];

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

  useEffect(() => {
    if (!session) return;
    const screens = new Set(session.allowed_screens || []);
    const allowed = TV_LINKS.filter((l) => screens.has(l.screen_key));
    // If only one TV screen allowed, redirect directly
    if (allowed.length === 1) {
      router.replace(allowed[0].href);
    }
  }, [session, router]);

  if (!session) {
    return <div style={{ padding: 40, textAlign: "center", color: "var(--muted)" }}>Carregando...</div>;
  }

  const screens = new Set(session.allowed_screens || []);
  const visibleLinks = TV_LINKS.filter((l) => screens.has(l.screen_key));

  const handleLogout = () => {
    clearAuth();
    router.push("/");
  };

  return (
    <div style={{ padding: 40, maxWidth: 600, margin: "0 auto" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
        <h1 style={{ fontSize: 28 }}>📺 Modo TV</h1>
        <button
          onClick={handleLogout}
          style={{
            padding: "8px 20px",
            background: "var(--color-negative)",
            color: "var(--on-accent)",
            border: "none",
            borderRadius: 8,
            cursor: "pointer",
            fontSize: 14,
          }}
        >
          Sair
        </button>
      </div>
      {visibleLinks.length === 0 ? (
        <p style={{ color: "var(--muted)" }}>Nenhuma tela TV permitida para este usuário.</p>
      ) : (
        <>
          <p style={{ color: "var(--muted)", marginBottom: 32 }}>
            Selecione a tela para exibição no ponto de venda:
          </p>
          <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
            {visibleLinks.map((link) => (
              <Link
                key={link.href}
                href={link.href}
                style={{
                  display: "block",
                  padding: "20px 24px",
                  background: "var(--surface-soft)",
                  borderRadius: 12,
                  color: "var(--text)",
                  textDecoration: "none",
                  fontSize: 18,
                }}
              >
                {link.label}
              </Link>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
