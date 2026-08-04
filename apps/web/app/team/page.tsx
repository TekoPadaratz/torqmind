"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";

import AppNav from "../components/AppNav";
import { setAuthToken } from "../lib/api";
import { getToken } from "../lib/auth";
import { buildUserLabel } from "../lib/format";
import { useEnsureScopedProductUrl } from "../lib/scope";
import { loadSession } from "../lib/session";
import TeamCostSection from "./TeamCostSection";

export const dynamic = "force-dynamic";

export default function TeamPage() {
  const router = useRouter();
  useEnsureScopedProductUrl();
  const [claims, setClaims] = useState<any>(null);

  useEffect(() => {
    const t = getToken();
    if (t) setAuthToken(t);
    loadSession(router, "product").then((me) => {
      if (me) setClaims(me);
    });
  }, [router]);

  const userLabel = useMemo(() => buildUserLabel(claims), [claims]);

  return (
    <div>
      <AppNav title="Equipe" userLabel={userLabel} />
      <div className="container">
        <div className="bi-grid" style={{ marginTop: 12 }}>
          <TeamCostSection />
        </div>
      </div>
    </div>
  );
}
