"use client";

import { useMemo } from "react";

import { buildMesesDisponiveis, clampAnoMes, currentAnoMesSP, fmtAnoMes } from "../../lib/month-year.mjs";

type Props = {
  value: number;
  onChange: (anoMes: number) => void;
  extraMonths?: number[];
  monthsBack?: number;
  label?: string;
  title?: string;
  "aria-label"?: string;
};

export default function MonthYearSelect({
  value,
  onChange,
  extraMonths,
  monthsBack = 18,
  label = "Mês",
  title,
  "aria-label": ariaLabel,
}: Props) {
  const now = currentAnoMesSP();
  const safeValue = clampAnoMes(value, now);
  const months = useMemo(
    () => buildMesesDisponiveis([safeValue, ...(extraMonths || [])], monthsBack, now),
    [safeValue, extraMonths, monthsBack, now],
  );

  return (
    <label className="profitScopeMonth" title={title || "Mês de referência"}>
      <span className="profitScopeMonthLabel">{label}</span>
      <select
        className="profitScopeMonthSelect"
        value={safeValue}
        onChange={(e) => onChange(clampAnoMes(Number(e.target.value), now))}
        aria-label={ariaLabel || label}
      >
        {months.map((m) => (
          <option key={m} value={m}>
            {fmtAnoMes(m)}
          </option>
        ))}
      </select>
    </label>
  );
}
