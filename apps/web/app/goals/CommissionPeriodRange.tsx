"use client";

import {
  formatCommissionPeriodLabel,
  validateCommissionPeriod,
} from "../lib/commission-period.mjs";

type Props = {
  dtIni: string;
  dtFim: string;
  onChange: (next: { dt_ini: string; dt_fim: string }) => void;
};

export default function CommissionPeriodRange({ dtIni, dtFim, onChange }: Props) {
  const error = validateCommissionPeriod(dtIni, dtFim);

  return (
    <div className="profitScopeToggles" role="group" aria-label="Período das comissões">
      <label className="profitScopeMonth" title="Data inicial do período">
        <span className="profitScopeMonthLabel">De</span>
        <input
          type="date"
          className="profitScopeMonthSelect"
          value={dtIni}
          onChange={(e) => onChange({ dt_ini: e.target.value, dt_fim: dtFim })}
          aria-label="Data inicial"
        />
      </label>
      <label className="profitScopeMonth" title="Data final do período">
        <span className="profitScopeMonthLabel">Até</span>
        <input
          type="date"
          className="profitScopeMonthSelect"
          value={dtFim}
          min={dtIni || undefined}
          onChange={(e) => onChange({ dt_ini: dtIni, dt_fim: e.target.value })}
          aria-label="Data final"
        />
      </label>
      <span className="muted" style={{ fontSize: 12, alignSelf: "center" }}>
        {error ? error : formatCommissionPeriodLabel(dtIni, dtFim)}
      </span>
    </div>
  );
}
