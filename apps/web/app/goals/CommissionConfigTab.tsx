"use client";

import { useCallback, useEffect, useState } from "react";
import { apiGet, apiPut } from "../lib/api";
import { formatCurrency } from "../lib/format";
import EmptyState from "../components/ui/EmptyState";

const TIER_STYLES: Record<string, { color: string; bg: string; icon: string }> = {
  bronze: { color: "#cd7f32", bg: "rgba(205,127,50,0.08)", icon: "🥉" },
  silver: { color: "#a0a0a0", bg: "rgba(160,160,160,0.08)", icon: "🥈" },
  gold: { color: "#d4a017", bg: "rgba(212,160,23,0.08)", icon: "🥇" },
  diamond: { color: "#4f9cf7", bg: "rgba(79,156,247,0.10)", icon: "💎" },
};

interface ConfigTabProps {
  idEmpresa: number | null;
  idFilial: number | null;
  onSaved?: () => void;
}

interface TierDraft {
  tier_key: string;
  tier_name: string;
  min_sales_amount: number;
  commission_percent: number;
  sort_order: number;
  is_active: boolean;
}

export default function CommissionConfigTab({ idEmpresa, idFilial, onSaved }: ConfigTabProps) {
  const [groups, setGroups] = useState<any[]>([]);
  const [tiers, setTiers] = useState<TierDraft[]>([]);
  const [paymentMode, setPaymentMode] = useState("team_total");
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
      const resp = await apiGet(`/bi/team/commissions/config?${params.toString()}`);
      setGroups(resp.groups || []);
      setTiers(resp.tiers || []);
      setPaymentMode(resp.config?.default_payment_mode || "team_total");
    } catch (err: any) {
      setError(err?.response?.data?.detail || "Falha ao carregar configuração.");
    } finally {
      setLoading(false);
    }
  }, [idEmpresa, idFilial]);

  useEffect(() => {
    fetchConfig();
  }, [fetchConfig]);

  const toggleGroup = (id: number) => {
    setGroups((prev) =>
      prev.map((g) => (g.id_grupo_produto === id ? { ...g, selected: !g.selected } : g))
    );
  };

  const updateTier = (index: number, field: keyof TierDraft, value: any) => {
    setTiers((prev) => prev.map((t, i) => (i === index ? { ...t, [field]: value } : t)));
  };

  const handleSave = async () => {
    if (!idFilial) return;
    setError("");
    setMessage("");

    // Validate
    const activeTiers = tiers.filter((t) => t.is_active);
    for (let i = 1; i < activeTiers.length; i++) {
      if (activeTiers[i].min_sales_amount <= activeTiers[i - 1].min_sales_amount) {
        setError("As faixas ativas devem estar em ordem crescente de valor mínimo.");
        return;
      }
    }
    for (const t of tiers) {
      if (t.min_sales_amount < 0) {
        setError("Valor mínimo deve ser positivo.");
        return;
      }
      if (t.commission_percent < 0) {
        setError("Percentual deve ser >= 0.");
        return;
      }
    }

    setSaving(true);
    try {
      const selectedGroups = groups
        .filter((g) => g.selected)
        .map((g) => ({ id_grupo_produto: g.id_grupo_produto, nome: g.nome }));
      const params = new URLSearchParams();
      params.set("id_filial", String(idFilial));
      if (idEmpresa) params.set("id_empresa", String(idEmpresa));
      await apiPut(`/bi/team/commissions/config?${params.toString()}`, {
        groups: selectedGroups,
        tiers,
        default_payment_mode: paymentMode,
      });
      setMessage("Configuração salva com sucesso!");
      if (onSaved) onSaved();
    } catch (err: any) {
      setError(err?.response?.data?.detail || "Falha ao salvar configuração.");
    } finally {
      setSaving(false);
    }
  };

  if (!idFilial) {
    return (
      <div className="card" style={{ marginTop: 16 }}>
        <EmptyState title="Selecione uma filial" detail="Escolha uma filial no painel lateral para configurar as comissões." />
      </div>
    );
  }

  if (loading) {
    return (
      <div className="card" style={{ marginTop: 16, padding: 32, textAlign: "center" }}>
        <div className="muted">Carregando configuração...</div>
      </div>
    );
  }

  const selectedCount = groups.filter((g) => g.selected).length;

  return (
    <div style={{ marginTop: 16 }}>
      {/* Explanation card */}
      <div className="card" style={{ padding: "14px 18px", borderLeft: "3px solid var(--color-accent, #3b82f6)" }}>
        <div style={{ fontWeight: 600, fontSize: 14, marginBottom: 4 }}>Configuração da Comissão</div>
        <div className="muted" style={{ fontSize: 13 }}>
          Escolha os grupos de produtos que contam para a comissão e defina as faixas de premiação.
          O sistema calcula automaticamente o nível atingido no mês selecionado.
        </div>
      </div>

      {error && <div className="card errorCard" style={{ marginTop: 12 }}>{error}</div>}
      {message && <div className="card" style={{ marginTop: 12, color: "#22c55e", fontWeight: 500, padding: "10px 14px" }}>{message}</div>}

      {/* Payment mode */}
      <div className="card" style={{ marginTop: 12 }}>
        <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 8 }}>Modo de pagamento padrão</div>
        <select
          value={paymentMode}
          onChange={(e) => setPaymentMode(e.target.value)}
          style={{ padding: "6px 10px", borderRadius: 6, border: "1px solid var(--border)", background: "var(--card-bg)", color: "inherit", width: "100%", maxWidth: 320 }}
        >
          <option value="team_total">Geral / Equipe</option>
          <option value="equal_split">Rateio igual por equipe</option>
          <option value="individual_sales">Individual por vendedor</option>
        </select>
        <div className="muted" style={{ fontSize: 11, marginTop: 4 }}>Define o modo padrão ao abrir a aba Comissões.</div>
      </div>

      {/* Groups selection */}
      <div className="card" style={{ marginTop: 12 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
          <div style={{ fontWeight: 600, fontSize: 13 }}>Grupos participantes</div>
          <span className="muted" style={{ fontSize: 12 }}>{selectedCount} selecionado(s)</span>
        </div>
        {selectedCount === 0 && (
          <div style={{ padding: "8px 12px", background: "rgba(234,179,8,0.08)", borderRadius: 6, fontSize: 12, marginBottom: 8, color: "#ca8a04" }}>
            Selecione os grupos que devem participar da comissão.
          </div>
        )}
        <div style={{ maxHeight: 300, overflowY: "auto", border: "1px solid var(--border)", borderRadius: 8, padding: "4px 0" }}>
          {groups.map((g) => (
            <label
              key={g.id_grupo_produto}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 8,
                padding: "6px 12px",
                cursor: "pointer",
                borderBottom: "1px solid rgba(255,255,255,0.04)",
              }}
            >
              <input
                type="checkbox"
                checked={!!g.selected}
                onChange={() => toggleGroup(g.id_grupo_produto)}
              />
              <span style={{ flex: 1, fontSize: 13 }}>{g.nome}</span>
              <span className="muted" style={{ fontSize: 11 }}>
                {g.faturamento_30d > 0 ? formatCurrency(g.faturamento_30d) + " /30d" : "—"}
              </span>
            </label>
          ))}
        </div>
      </div>

      {/* Tiers */}
      <div className="card" style={{ marginTop: 12 }}>
        <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 12 }}>Níveis de premiação</div>
        <div style={{ display: "grid", gap: 10 }}>
          {tiers.map((tier, i) => {
            const style = TIER_STYLES[tier.tier_key] || TIER_STYLES.bronze;
            return (
              <div
                key={tier.tier_key}
                style={{
                  display: "grid",
                  gridTemplateColumns: "auto 1fr 140px 100px auto",
                  gap: 10,
                  alignItems: "center",
                  padding: "10px 14px",
                  borderRadius: 10,
                  background: tier.is_active ? style.bg : "rgba(100,100,100,0.04)",
                  border: `1px solid ${tier.is_active ? style.color + "30" : "var(--border)"}`,
                  opacity: tier.is_active ? 1 : 0.5,
                }}
              >
                <span style={{ fontSize: 18 }}>{style.icon}</span>
                <div>
                  <input
                    type="text"
                    value={tier.tier_name}
                    onChange={(e) => updateTier(i, "tier_name", e.target.value)}
                    style={{
                      background: "transparent",
                      border: "none",
                      borderBottom: "1px solid var(--border)",
                      color: style.color,
                      fontWeight: 700,
                      fontSize: 14,
                      width: "100%",
                      maxWidth: 140,
                    }}
                  />
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
                  <span style={{ fontSize: 11, color: "var(--text-muted)" }}>R$</span>
                  <input
                    type="number"
                    min={0}
                    step={1000}
                    value={tier.min_sales_amount}
                    onChange={(e) => updateTier(i, "min_sales_amount", Number(e.target.value))}
                    style={{
                      width: 100,
                      padding: "4px 6px",
                      borderRadius: 4,
                      border: "1px solid var(--border)",
                      background: "var(--card-bg)",
                      color: "inherit",
                      fontSize: 13,
                    }}
                  />
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
                  <input
                    type="number"
                    min={0}
                    max={100}
                    step={0.1}
                    value={tier.commission_percent}
                    onChange={(e) => updateTier(i, "commission_percent", Number(e.target.value))}
                    style={{
                      width: 60,
                      padding: "4px 6px",
                      borderRadius: 4,
                      border: "1px solid var(--border)",
                      background: "var(--card-bg)",
                      color: "inherit",
                      fontSize: 13,
                    }}
                  />
                  <span style={{ fontSize: 11, color: "var(--text-muted)" }}>%</span>
                </div>
                <label style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 11, cursor: "pointer" }}>
                  <input
                    type="checkbox"
                    checked={tier.is_active}
                    onChange={(e) => updateTier(i, "is_active", e.target.checked)}
                  />
                  Ativo
                </label>
              </div>
            );
          })}
        </div>
        <div className="muted" style={{ fontSize: 11, marginTop: 8 }}>
          Venda elegível é o total vendido nos grupos selecionados. O nível é definido pelo total vendido da filial no mês.
        </div>
      </div>

      {/* Save button */}
      <div style={{ marginTop: 16, display: "flex", gap: 12 }}>
        <button
          className="btn"
          onClick={handleSave}
          disabled={saving}
          style={{ padding: "10px 24px", fontWeight: 600 }}
        >
          {saving ? "Salvando..." : "Salvar configuração"}
        </button>
      </div>
    </div>
  );
}
