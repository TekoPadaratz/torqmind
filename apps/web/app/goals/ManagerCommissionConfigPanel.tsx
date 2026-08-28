"use client";

import { useCallback, useEffect, useState, type CSSProperties } from "react";
import { apiGet, apiPut } from "../lib/api";
import { extractApiError } from "../lib/errors";
import EmptyState from "../components/ui/EmptyState";

interface Props {
  idEmpresa: number | null;
  idFilial: number | null;
  onSaved?: () => void;
}

type GroupRow = { id_grupo_produto: number; nome: string; selected: boolean };

const groupGridStyle: CSSProperties = {
  maxHeight: 220,
  overflow: "auto",
  border: "1px solid var(--border)",
  borderRadius: 8,
  padding: 8,
  display: "grid",
  gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))",
  gap: "8px 12px",
  alignItems: "start",
};

const groupLabelStyle: CSSProperties = {
  display: "flex",
  alignItems: "flex-start",
  gap: 8,
  fontSize: 12,
  cursor: "pointer",
  minWidth: 0,
  lineHeight: 1.35,
};

export default function ManagerCommissionConfigPanel({ idEmpresa, idFilial, onSaved }: Props) {
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [rate, setRate] = useState(2);
  const [salesGroups, setSalesGroups] = useState<GroupRow[]>([]);
  const [lossGroups, setLossGroups] = useState<GroupRow[]>([]);
  const [cashNote, setCashNote] = useState("");

  const fetchConfig = useCallback(async () => {
    if (!idFilial) return;
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams();
      params.set("id_filial", String(idFilial));
      if (idEmpresa) params.set("id_empresa", String(idEmpresa));
      const resp = await apiGet(`/bi/team/manager-commissions/config?${params.toString()}`);
      setRate(Number(resp?.config?.default_rate_pct ?? 2));
      setSalesGroups(resp?.sales_base_groups || []);
      setLossGroups(resp?.stock_loss_groups || []);
      setCashNote(String(resp?.cash_adj_note || ""));
    } catch (err: any) {
      setError(extractApiError(err, "Falha ao carregar config de gerente."));
    } finally {
      setLoading(false);
    }
  }, [idEmpresa, idFilial]);

  useEffect(() => {
    fetchConfig();
  }, [fetchConfig]);

  const toggle = (kind: "sales" | "loss", id: number) => {
    const setter = kind === "sales" ? setSalesGroups : setLossGroups;
    setter((prev) =>
      prev.map((g) => (g.id_grupo_produto === id ? { ...g, selected: !g.selected } : g)),
    );
  };

  const selectAll = (kind: "sales" | "loss", selected: boolean) => {
    const setter = kind === "sales" ? setSalesGroups : setLossGroups;
    setter((prev) => prev.map((g) => ({ ...g, selected })));
  };

  const handleSave = async () => {
    if (!idFilial) return;
    setSaving(true);
    setError("");
    setMessage("");
    try {
      const params = new URLSearchParams();
      params.set("id_filial", String(idFilial));
      if (idEmpresa) params.set("id_empresa", String(idEmpresa));
      await apiPut(`/bi/team/manager-commissions/config?${params.toString()}`, {
        default_rate_pct: rate,
        sales_base_groups: salesGroups,
        stock_loss_groups: lossGroups,
      });
      setMessage("Configuração de gerente salva.");
      onSaved?.();
    } catch (err: any) {
      setError(extractApiError(err, "Falha ao salvar."));
    } finally {
      setSaving(false);
    }
  };

  if (!idFilial) {
    return (
      <div className="card" style={{ marginTop: 16 }}>
        <EmptyState title="Selecione uma filial" detail="Escolha a filial para configurar a comissão de gerentes." />
      </div>
    );
  }

  const renderGroupList = (kind: "sales" | "loss", groups: GroupRow[]) => (
    <div>
      <div style={{ display: "flex", gap: 8, marginBottom: 8 }}>
        <button type="button" className="btn" onClick={() => selectAll(kind, true)} style={{ fontSize: 12, padding: "4px 8px" }}>
          Todos
        </button>
        <button type="button" className="btn" onClick={() => selectAll(kind, false)} style={{ fontSize: 12, padding: "4px 8px" }}>
          Nenhum
        </button>
        <span className="muted" style={{ fontSize: 12, alignSelf: "center" }}>
          {groups.filter((g) => g.selected).length}/{groups.length} selecionados
        </span>
      </div>
      <div style={groupGridStyle}>
        {groups.map((g) => (
          <label key={`${kind}-${g.id_grupo_produto}`} style={groupLabelStyle}>
            <input
              type="checkbox"
              checked={g.selected}
              onChange={() => toggle(kind, g.id_grupo_produto)}
              style={{ width: 14, height: 14, flexShrink: 0, marginTop: 2 }}
            />
            <span style={{ minWidth: 0, wordBreak: "break-word" }}>
              {g.nome || `Grupo ${g.id_grupo_produto}`}
            </span>
          </label>
        ))}
      </div>
    </div>
  );

  return (
    <div className="card" style={{ marginTop: 16 }}>
      <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 4 }}>Comissão de gerentes</div>
      <p className="muted" style={{ fontSize: 12, marginTop: 0 }}>
        Base da loja por grupos (não individual). Taxa padrão e filtros de venda / perda de estoque.
      </p>

      {error ? <div className="errorCard" style={{ marginBottom: 8 }}>{String(error)}</div> : null}
      {message ? <div className="muted" style={{ marginBottom: 8 }}>{message}</div> : null}

      {loading ? (
        <div className="muted">Carregando…</div>
      ) : (
        <>
          <div style={{ marginBottom: 16 }}>
            <label style={{ fontSize: 12, fontWeight: 600 }}>Taxa de comissão padrão (%)</label>
            <input
              type="number"
              min={0}
              max={100}
              step={0.01}
              value={rate}
              onChange={(e) => setRate(Number(e.target.value))}
              style={{
                display: "block",
                marginTop: 4,
                width: 120,
                padding: "6px 8px",
                borderRadius: 6,
                border: "1px solid var(--border)",
                background: "var(--card-bg)",
              }}
            />
          </div>

          <div style={{ marginBottom: 16 }}>
            <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 6 }}>A. Vendas base (comissão bruta)</div>
            <p className="muted" style={{ fontSize: 11, marginTop: 0 }}>
              Pré-seleção exclui grupos 1–4, 7–10, 16, 39, 40.
            </p>
            {renderGroupList("sales", salesGroups)}
          </div>

          <div style={{ marginBottom: 16 }}>
            <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 6 }}>B. Notas de perda de estoque</div>
            <p className="muted" style={{ fontSize: 11, marginTop: 0 }}>
              CFOP 5.927 · INSUMOS removido por padrão.
            </p>
            {renderGroupList("loss", lossGroups)}
          </div>

          {cashNote ? (
            <p className="muted" style={{ fontSize: 11 }}>{cashNote}</p>
          ) : null}

          <button type="button" className="btn" disabled={saving} onClick={handleSave}>
            {saving ? "Salvando…" : "Salvar config de gerente"}
          </button>
        </>
      )}
    </div>
  );
}
