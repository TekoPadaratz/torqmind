"use client";

import AppNav from "../../components/AppNav";
import { buildUserLabel } from "../../lib/format";
import { readCachedSession } from "../../lib/session";
import SalesAbcSection from "../SalesAbcSection";

export const dynamic = "force-dynamic";

export default function SalesAbcPage() {
  const userLabel = buildUserLabel(readCachedSession());

  return (
    <div>
      <AppNav title="Curva ABC" userLabel={userLabel} />
      <div className="container">
        <div className="bi-grid" style={{ marginTop: 12 }}>
          <SalesAbcSection />
        </div>
      </div>
    </div>
  );
}
