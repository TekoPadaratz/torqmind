"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { apiGet, apiPut } from "../lib/api";
import EmptyState from "../components/ui/EmptyState";
import GridSearchInput from "../components/ui/GridSearchInput";
import { useGridSearch } from "../lib/use-grid-search";

interface BudgetConfigTabProps {
  idEmpresa: number | null;
  idFilial: number | null;
}

interface AccountRow {
  id_plano_conta: number;
  codigo: string;
  nome_conta: string;
  valor_max: number;
  alerta_pct: number;
  configurado: boolean;
}

function parseNum(text: string): number {
  const clean = String(text).replace(/[^\d.,]/g, "").replace(/\./g, "").replace(",", ".");
  const n = parseFloat(clean);
  return Number.isFinite(n) ? n : 0;
}

export default function BudgetConfigTab({ idEmpresa, idFilial }: BudgetConfigTabProps) {
  const [accounts, setAccounts] = useState<AccountRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const fetchConfig = useCallback(async () => {
    if (!idFilial) return;
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams();
      params.set("id_filial", String(idFilial));
      if (idEmpresa) params.set("id_empresa", String(idEmpresa));
      const resp = await apiGet(`/bi/budget/config?${params.toString()}`);
      setAccounts((resp.accounts || []).map((a: any) => ({
        id_plano_conta: a.id_plano_conta,
        codigo: a.codigo || "",
        nome_conta: a.nome_conta || "",
        valor_max: Number(a.valor_max || 0),
        alerta_pct: Number(a.alerta_pct || 90),
        configurado: Boolean(a.configurado),
      })));
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || err?.response?.data?.detail || "Falha ao carregar contas.");
    } finally {
      setLoading(false);
    }
  }, [idEmpresa, idFilial]);

  useEffect(() => {
    fetchConfig();
  }, [fetchConfig]);

  const update = (id: number, field: "valor_max" | "alerta_pct", value: number) => {
    setAccounts((prev) => prev.map((a) => (a.id_plano_conta === id ? { ...a, [field]: value } : a)));
  };

  const { query, setQuery, filteredRows } = useGridSearch(
    accounts as unknown as Record<string, unknown>[],
  );

  const configuredCount = useMemo(() => accounts.filter((a) => a.valor_max > 0).length, [accounts]);

  const handleSave = async () => {
    if (!idFilial) return;
    setSaving(true);
    setError("");
    setMessage("");
    try {
      const params = new URLSearchParams();
      params.set("id_filial", String(idFilial));
      if (idEmpresa) params.set("id_empresa", String(idEmpresa));
      // Envia apenas contas com teto ou que estavam configuradas (para permitir remover).
      const items = accounts
        .filter((a) => a.valor_max > 0 || a.configurado)
        .map((a) => ({
          id_plano_conta: a.id_plano_conta,
          valor_max: Number(a.valor_max || 0),
          alerta_pct: Math.max(1, Math.min(100, Number(a.alerta_pct || 90))),
        }));
      await apiPut(`/bi/budget/config?${params.toString()}`, { items });
      setMessage("Orçamento salvo.");
      await fetchConfig();
    } catch (err: any) {
      setError(err?.response?.data?.detail?.message || err?.response?.data?.detail || "Falha ao salvar.");
    } finally {
      setSaving(false);
    }
  };

  if (!idFilial) {
    return (
      <div className="card col-12">
        <EmptyState
          title="Selecione uma filial."
          detail="A gestão orçamentária é configurada por posto. Escolha exatamente 1 filial no seletor acima para definir os tetos de despesa."
        />
      </div>
    );
  }

  return (
    <div className="card col-12">
      <div className="sectionEyebrow">Gestão Orçamentária</div>
      <h2 style={{ marginTop: 4 }}>Teto de despesa por conta</h2>
      <div className="muted" style={{ marginTop: 8, fontSize: 13 }}>
        Defina um teto mensal (R$) para cada conta de despesa e a partir de quantos % o sistema deve
        avisar. As contas vêm do plano de contas do Xpert e sincronizam sozinhas. Deixe o teto em 0
        para a conta ficar fora do orçamento.
      </div>

      <div style={{ display: "flex", gap: 10, alignItems: "center", marginTop: 14, flexWrap: "wrap" }}>
        <GridSearchInput value={query} onChange={setQuery} />
        <span className="muted" style={{ fontSize: 12 }}>
          {configuredCount} conta(s) com teto · {accounts.length} no total
        </span>
        <button className="btn" type="button" onClick={handleSave} disabled={saving || loading} style={{ marginLeft: "auto" }}>
          {saving ? "Salvando..." : "Salvar orçamento"}
        </button>
      </div>

      {error ? <div className="card errorCard" style={{ marginTop: 12 }}>{error}</div> : null}
      {message ? <div className="muted" style={{ marginTop: 10, color: "var(--color-positive)" }}>{message}</div> : null}

      {loading && !accounts.length ? (
        <p className="muted" style={{ marginTop: 12 }}>Carregando contas...</p>
      ) : (
        <div className="tableScroll" style={{ marginTop: 12, maxHeight: 520, overflowY: "auto" }}>
          <table className="table compact">
            <thead>
              <tr>
                <th>Código</th>
                <th>Conta</th>
                <th>Teto mensal (R$)</th>
                <th>Alerta (%)</th>
              </tr>
            </thead>
            <tbody>
              {filteredRows.map((a: any) => (
                <tr key={a.id_plano_conta}>
                  <td className="muted" style={{ fontSize: 11 }}>{a.codigo}</td>
                  <td>{a.nome_conta}</td>
                  <td>
                    <input
                      className="input"
                      type="text"
                      inputMode="decimal"
                      value={a.valor_max ? a.valor_max.toLocaleString("pt-BR") : ""}
                      placeholder="0"
                      onChange={(e) => update(a.id_plano_conta, "valor_max", parseNum(e.target.value))}
                      style={{ width: 130, textAlign: "right" }}
                    />
                  </td>
                  <td>
                    <input
                      className="input"
                      type="number"
                      min={1}
                      max={100}
                      value={a.alerta_pct}
                      onChange={(e) => update(a.id_plano_conta, "alerta_pct", parseInt(e.target.value) || 90)}
                      style={{ width: 72, textAlign: "center" }}
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
