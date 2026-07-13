"use client";

import { useEffect, useRef, useState } from "react";
import { formatCurrency } from "../lib/format";
import { apiPost } from "../lib/api";

// Cores no padrão do sistema (Ativo Circulante = positivo/verde, Não-Circulante
// = cobre/âmbar, Passivo = negativo/vermelho). Separadas, sem virar circo.
const GRUPO = {
  ativo_circulante: { cor: "var(--color-positive)", bg: "rgba(34,197,94,0.06)" },
  ativo_nao_circulante: { cor: "var(--accent-copper, #b8722c)", bg: "rgba(184,114,44,0.07)" },
  passivo_circulante: { cor: "var(--color-negative)", bg: "rgba(239,68,68,0.06)" },
} as const;

type Item = { id?: number; label: string; valor: number; qtd?: number | null; origem: string; editavel: boolean };
type Secao = { secao: string; label: string; total: number; itens: Item[]; editavel: boolean; id_tipo?: number | null; ordem: number };
type Grupo = { label: string; total: number; secoes: Secao[] };
type Filial = {
  id_filial: number;
  nome: string;
  grupos: Record<string, Grupo>;
  totais: {
    ativo_circulante: number;
    ativo_nao_circulante: number;
    ativo_total: number;
    passivo: number;
    capital_giro: number;
    liquidez_corrente: number | null;
    cobre_passivo: boolean;
  };
};

function fmtMonth(am: number): string {
  const s = String(am);
  return `${s.slice(4, 6)}/${s.slice(0, 4)}`;
}

function EditableSecao({
  filial,
  secao,
  anoMes,
  idEmpresa,
  onSaved,
}: {
  filial: number;
  secao: Secao;
  anoMes: number;
  idEmpresa?: number;
  onSaved: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [pinned, setPinned] = useState(false);
  const [rows, setRows] = useState<{ descricao: string; valor: string }[]>([]);
  const [saving, setSaving] = useState(false);
  const closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (open) {
      const base = secao.itens.map((i) => ({ descricao: i.label, valor: String(i.valor ?? "") }));
      setRows(base.length ? base : [{ descricao: "", valor: "" }]);
    }
  }, [open, secao.itens]);

  const scheduleClose = () => {
    if (pinned) return;
    closeTimer.current = setTimeout(() => setOpen(false), 220);
  };
  const cancelClose = () => {
    if (closeTimer.current) clearTimeout(closeTimer.current);
  };

  const save = async () => {
    if (!idEmpresa) return;
    setSaving(true);
    try {
      await apiPost(`/bi/profit-management/solvencia/manual${idEmpresa ? `?id_empresa=${idEmpresa}` : ""}`, {
        id_filial: filial,
        ano_mes: anoMes,
        id_tipo: secao.id_tipo,
        itens: rows
          .filter((r) => r.descricao.trim())
          .map((r) => ({ descricao: r.descricao.trim(), valor: Number(String(r.valor).replace(/\./g, "").replace(",", ".")) || 0 })),
      });
      setPinned(false);
      setOpen(false);
      onSaved();
    } finally {
      setSaving(false);
    }
  };

  const vazio = secao.itens.length === 0;

  return (
    <div
      style={{ position: "relative" }}
      onMouseEnter={() => {
        cancelClose();
        setOpen(true);
      }}
      onMouseLeave={scheduleClose}
    >
      <button
        type="button"
        onClick={() => {
          setPinned(true);
          setOpen(true);
        }}
        title="Clique para preencher / editar"
        style={{
          display: "flex",
          width: "100%",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 8,
          padding: "8px 10px",
          border: "1px dashed var(--color-border)",
          borderRadius: 8,
          background: "var(--color-surface)",
          color: "inherit",
          cursor: "pointer",
          textAlign: "left",
        }}
      >
        <span style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 13, fontWeight: 500 }}>
          {secao.label}
          <span aria-hidden style={{ fontSize: 11, opacity: 0.7 }}>✎</span>
        </span>
        <span style={{ fontSize: 14, fontWeight: 600, fontVariantNumeric: "tabular-nums" }}>
          {vazio ? <span style={{ fontSize: 12, opacity: 0.7, fontWeight: 400 }}>clique para preencher</span> : formatCurrency(secao.total)}
        </span>
      </button>

      {open && (
        <div
          onMouseEnter={cancelClose}
          onMouseLeave={scheduleClose}
          style={{
            position: "absolute",
            zIndex: 30,
            top: "calc(100% + 4px)",
            left: 0,
            right: 0,
            minWidth: 260,
            background: "var(--color-surface)",
            border: "1px solid var(--color-border)",
            borderRadius: 10,
            boxShadow: "0 12px 32px rgba(0,0,0,0.18)",
            padding: 12,
          }}
        >
          <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 8, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span>{secao.label} — {fmtMonth(anoMes)}</span>
            <button type="button" onClick={() => { setPinned(false); setOpen(false); }} style={{ border: "none", background: "transparent", cursor: "pointer", opacity: 0.6, fontSize: 14 }}>✕</button>
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 6, maxHeight: 240, overflowY: "auto" }}>
            {rows.map((r, i) => (
              <div key={i} style={{ display: "flex", gap: 6 }}>
                <input
                  placeholder="Nome"
                  value={r.descricao}
                  onChange={(e) => setRows((rs) => rs.map((x, j) => (j === i ? { ...x, descricao: e.target.value } : x)))}
                  style={{ flex: 1, minWidth: 0, padding: "5px 8px", borderRadius: 6, border: "1px solid var(--color-border)", background: "var(--color-bg, transparent)", color: "inherit", fontSize: 12 }}
                />
                <input
                  placeholder="0,00"
                  inputMode="decimal"
                  value={r.valor}
                  onChange={(e) => setRows((rs) => rs.map((x, j) => (j === i ? { ...x, valor: e.target.value } : x)))}
                  style={{ width: 96, padding: "5px 8px", borderRadius: 6, border: "1px solid var(--color-border)", background: "var(--color-bg, transparent)", color: "inherit", fontSize: 12, textAlign: "right" }}
                />
                <button type="button" onClick={() => setRows((rs) => rs.filter((_, j) => j !== i))} title="Remover" style={{ border: "none", background: "transparent", cursor: "pointer", opacity: 0.5, fontSize: 14 }}>✕</button>
              </div>
            ))}
          </div>
          <button
            type="button"
            onClick={() => setRows((rs) => [...rs, { descricao: "", valor: "" }])}
            style={{ marginTop: 8, border: "1px dashed var(--color-border)", background: "transparent", color: "inherit", borderRadius: 6, padding: "5px 8px", cursor: "pointer", fontSize: 12, width: "100%" }}
          >
            + adicionar linha
          </button>
          <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 10 }}>
            <button type="button" onClick={() => { setPinned(false); setOpen(false); }} style={{ border: "1px solid var(--color-border)", background: "transparent", color: "inherit", borderRadius: 6, padding: "6px 12px", cursor: "pointer", fontSize: 12 }}>Cancelar</button>
            <button type="button" onClick={save} disabled={saving} style={{ border: "none", background: "var(--color-positive)", color: "#fff", borderRadius: 6, padding: "6px 14px", cursor: "pointer", fontSize: 12, fontWeight: 600, opacity: saving ? 0.6 : 1 }}>{saving ? "Salvando…" : "Salvar"}</button>
          </div>
        </div>
      )}
    </div>
  );
}

function GrupoPanel({ grupoKey, grupo, filial, anoMes, idEmpresa, onSaved }: { grupoKey: string; grupo: Grupo; filial: number; anoMes: number; idEmpresa?: number; onSaved: () => void }) {
  const c = (GRUPO as any)[grupoKey] || GRUPO.ativo_circulante;
  return (
    <div className="card" style={{ padding: 0, overflow: "hidden", borderTop: `3px solid ${c.cor}` }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", padding: "12px 16px", background: c.bg }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: c.cor, textTransform: "uppercase", letterSpacing: 0.3 }}>{grupo.label}</div>
        <div style={{ fontSize: 18, fontWeight: 700, color: c.cor, fontVariantNumeric: "tabular-nums" }}>{formatCurrency(grupo.total)}</div>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 8, padding: 14 }}>
        {grupo.secoes.length === 0 && <div style={{ fontSize: 12, opacity: 0.6 }}>Sem itens.</div>}
        {grupo.secoes.map((secao) =>
          secao.editavel ? (
            <EditableSecao key={secao.secao} filial={filial} secao={secao} anoMes={anoMes} idEmpresa={idEmpresa} onSaved={onSaved} />
          ) : (
            <div key={secao.secao} style={{ border: "1px solid var(--color-border-subtle, var(--color-border))", borderRadius: 8, overflow: "hidden" }}>
              <div style={{ display: "flex", justifyContent: "space-between", padding: "7px 10px", fontSize: 13, fontWeight: 600, background: "var(--color-surface-muted, transparent)" }}>
                <span>{secao.label}</span>
                <span style={{ fontVariantNumeric: "tabular-nums" }}>{formatCurrency(secao.total)}</span>
              </div>
              {secao.itens.length > 1 && (
                <div style={{ padding: "2px 10px 6px" }}>
                  {secao.itens.map((it, i) => (
                    <div key={i} style={{ display: "flex", justifyContent: "space-between", fontSize: 12, padding: "3px 0", color: "var(--color-text-secondary)" }}>
                      <span>{it.label}{it.qtd ? ` · ${it.qtd.toLocaleString("pt-BR", { maximumFractionDigits: 0 })} L` : ""}</span>
                      <span style={{ fontVariantNumeric: "tabular-nums" }}>{formatCurrency(it.valor)}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )
        )}
      </div>
    </div>
  );
}

// Cartão-indicador com rótulo, valor e uma linha curta explicando a fórmula.
function Kpi({ label, sub, value, color }: { label: string; sub: string; value: string; color: string }) {
  return (
    <div style={{ padding: "10px 12px", borderRadius: 10, background: "var(--color-surface-muted, rgba(127,127,127,0.05))", border: "1px solid var(--color-border-subtle, var(--color-border))" }}>
      <div className="sectionEyebrow">{label}</div>
      <div style={{ fontSize: 22, fontWeight: 700, marginTop: 2, color, fontVariantNumeric: "tabular-nums" }}>{value}</div>
      <div style={{ fontSize: 11, color: "var(--color-text-secondary)", marginTop: 2 }}>{sub}</div>
    </div>
  );
}

export function SolvenciaDetalhada({
  data,
  monthValue,
  onMonthChange,
  idEmpresa,
  onSaved,
}: {
  data: any;
  monthValue: number | null;
  onMonthChange: (m: number) => void;
  idEmpresa?: number;
  onSaved: () => void;
}) {
  if (!data) return null;
  const anoMes: number = monthValue ?? data.ano_mes;
  // Ordena por nome (natural: VR 2 antes de VR 10), não por código.
  const filiais: Filial[] = [...(data.filiais || [])].sort((a, b) =>
    (a.nome || "").localeCompare(b.nome || "", "pt-BR", { numeric: true, sensitivity: "base" }),
  );

  return (
    <div style={{ marginTop: 16 }}>
      <div className="card" style={{ padding: 16, display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 12 }}>
        <div>
          <div className="sectionEyebrow">Solvência — Posição Atual</div>
          <div style={{ fontSize: 13, color: "var(--color-text-secondary)" }}>
            Ativos disponíveis, recebíveis e estoque frente às obrigações do mês.
          </div>
        </div>
        <label style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13, fontWeight: 500 }}>
          <span>Mês:</span>
          <select
            value={anoMes}
            onChange={(e) => onMonthChange(Number(e.target.value))}
            style={{ padding: "6px 12px", borderRadius: 8, border: "1px solid var(--color-border)", background: "var(--color-surface)", color: "inherit", fontSize: 13 }}
          >
            {(data.meses_disponiveis || [anoMes]).map((m: number) => (
              <option key={m} value={m}>{fmtMonth(m)}</option>
            ))}
          </select>
        </label>
      </div>

      <div style={{ fontSize: 12, color: "var(--color-text-secondary)", marginTop: 10, display: "flex", alignItems: "center", gap: 6 }}>
        <span aria-hidden>✎</span> Passe o mouse ou clique nos painéis de Bancos e Investimentos para preencher os valores do mês.
      </div>
      <div style={{ fontSize: 12, color: "var(--color-text-secondary)", marginTop: 6, lineHeight: 1.5 }}>
        <strong>Capital de giro</strong> = Ativo − Passivo (o que sobra depois de quitar as contas).{" "}
        <strong>Liquidez</strong> = Ativo ÷ Passivo, em vezes: <strong>1,00×</strong> cobre exatamente as contas; acima de 1 sobra caixa, abaixo de 1 falta.
      </div>

      {filiais.map((f) => {
        const t = f.totais;
        const liq = t.liquidez_corrente;
        const cobre = t.cobre_passivo;
        // Quadro geral da filial: agrupa totalizadores + os 3 painéis num só bloco.
        // Não usa a classe .card (backdrop-filter) para não prender os popovers de edição.
        return (
          <div
            key={f.id_filial}
            style={{
              marginTop: 18,
              padding: 18,
              borderRadius: 14,
              border: "1px solid var(--color-border)",
              borderLeft: `4px solid ${cobre ? "var(--color-positive)" : "var(--color-negative)"}`,
              background: "var(--color-surface, transparent)",
            }}
          >
            {/* Cabeçalho da filial + veredito */}
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 8, marginBottom: 14 }}>
              <div style={{ fontSize: 16, fontWeight: 700 }}>{f.nome}</div>
              <span
                style={{
                  fontSize: 12,
                  fontWeight: 600,
                  padding: "3px 10px",
                  borderRadius: 999,
                  color: cobre ? "var(--color-positive)" : "var(--color-negative)",
                  background: cobre ? "rgba(34,197,94,0.10)" : "rgba(239,68,68,0.10)",
                }}
              >
                {cobre ? "Ativos cobrem o passivo" : "Ativos não cobrem o passivo"}
              </span>
            </div>

            {/* Totalizadores */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12 }}>
              <Kpi label="Ativo Total" sub="Circulante + investimentos" value={formatCurrency(t.ativo_total)} color="var(--color-positive)" />
              <Kpi label="Passivo" sub="Contas a pagar em aberto" value={formatCurrency(t.passivo)} color="var(--color-negative)" />
              <Kpi label="Capital de Giro" sub="Ativo − Passivo" value={formatCurrency(t.capital_giro)} color={t.capital_giro >= 0 ? "var(--color-positive)" : "var(--color-negative)"} />
              <Kpi label="Liquidez" sub="Ativo ÷ Passivo" value={liq != null ? `${liq.toFixed(2).replace(".", ",")}×` : "—"} color={(liq ?? 0) >= 1 ? "var(--color-positive)" : "var(--color-negative)"} />
            </div>

            {/* Grupos */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))", gap: 14, marginTop: 14, alignItems: "start" }}>
              {(["ativo_circulante", "ativo_nao_circulante", "passivo_circulante"] as const).map((gk) =>
                f.grupos[gk] ? (
                  <GrupoPanel key={gk} grupoKey={gk} grupo={f.grupos[gk]} filial={f.id_filial} anoMes={anoMes} idEmpresa={idEmpresa} onSaved={onSaved} />
                ) : null
              )}
            </div>
          </div>
        );
      })}

      {filiais.length === 0 && (
        <div className="card" style={{ marginTop: 16, padding: 20, textAlign: "center", color: "var(--color-text-secondary)" }}>
          Sem dados de solvência para o mês selecionado.
        </div>
      )}
    </div>
  );
}
