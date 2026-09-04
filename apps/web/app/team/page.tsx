"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";

import AppNav from "../components/AppNav";
import { setAuthToken } from "../lib/api";
import { getToken } from "../lib/auth";
import { buildUserLabel } from "../lib/format";
import { useEnsureScopedProductUrl } from "../lib/scope";
import { loadSession } from "../lib/session";
import MonthYearSelect from "../components/ui/MonthYearSelect";
import { currentAnoMesSP } from "../lib/month-year.mjs";
import TeamCostSection from "./TeamCostSection";
import TeamFuelDashboard from "./TeamFuelDashboard";

export const dynamic = "force-dynamic";

export default function TeamPage() {
  const router = useRouter();
  useEnsureScopedProductUrl();
  const [claims, setClaims] = useState<any>(null);
  const [anoMes, setAnoMes] = useState<number>(() => currentAnoMesSP());

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
        <div style={{ marginTop: 12, marginBottom: 8 }}>
          <MonthYearSelect
            value={anoMes}
            onChange={setAnoMes}
            title="Mês de referência da equipe"
            aria-label="Mês da equipe"
          />
        </div>
        <div className="bi-grid">
          <TeamFuelDashboard anoMes={anoMes} />
          <div className="col-12">
            <TeamCostSection anoMes={anoMes} hideMonthSelect />
          </div>
        </div>
      </div>
    </div>
  );
}
