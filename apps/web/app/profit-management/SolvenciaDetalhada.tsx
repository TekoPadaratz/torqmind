"use client";

import { useEffect, useRef, useState } from "react";
import { formatCurrency } from "../lib/format";
import { apiPost } from "../lib/api";
import PortalDropdown from "../components/ui/PortalDropdown";

/** Máscara pt-BR ao digitar: dígitos → "1.234,56" */
function formatCurrencyMask(raw: string): string {
  const digits = String(raw ?? "").replace(/\D/g, "");
  if (!digits) return "";
  const cents = Number.parseInt(digits, 10);
  if (!Number.isFinite(cents)) return "";
  return (cents / 100).toLocaleString("pt-BR", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function parseCurrencyMask(text: string): number {
  const normalized = String(text ?? "")
    .trim()
    .replace(/\./g, "")
    .replace(",", ".");
  const n = Number(normalized);
  return Number.isFinite(n) ? n : 0;
}

function numberToMask(value: number | string | null | undefined): string {
  if (value === null || value === undefined || value === "") return "";
  const n = typeof value === "number" ? value : parseCurrencyMask(String(value));
  if (!Number.isFinite(n)) return "";
  return n.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

const GRUPO = {
  ativo_circulante: { cor: "var(--color-positive)", bg: "rgba(34,197,94,0.06)" },
  ativo_nao_circulante: { cor: "var(--accent-copper, #b8722c)", bg: "rgba(184,114,44,0.07)" },
  passivo_circulante: { cor: "var(--color-negative)", bg: "rgba(239,68,68,0.06)" },
} as const;

type Item = { id?: number; label: string; valor: number; qtd?: number | null; origem: string; editavel: boolean; as_of?: boolean; editado_humano?: boolean; valor_sistema?: number };
type Secao = {
  secao: string;
  label: string;
  total: number;
  itens: Item[];
  hint_itens?: { label: string; valor: number; qtd?: number | null }[];
  colapsado?: boolean;
  editavel: boolean;
  id_tipo?: number | null;
  ordem: number;
  editado_humano?: boolean;
  valor_sistema?: number;
};
type Grupo = { label: string; total: number; secoes: Secao[] };
type Filial = {
  id_filial: number;
  nome: string;
  grupos: Record<string, Grupo>;
  totais: {
    ativo_circulante: number;
    ativo_nao_circulante: number;
    ativo_total: number;
    ativo_com_estoque?: number;
    ativo_sem_estoque?: number;
    estoque_total?: number;
    passivo: number;
    capital_giro: number;
    capital_giro_com_estoque?: number;
    capital_giro_sem_estoque?: number;
    liquidez_corrente: number | null;
    liquidez_sem_estoque?: number | null;
    cobre_passivo: boolean;
  };
};

function fmtMonth(am: number): string {
  const s = String(am);
  return `${s.slice(4, 6)}/${s.slice(0, 4)}`;
}

function HintSecao({ secao }: { secao: Secao }) {
  const [open, setOpen] = useState(false);
  const anchorRef = useRef<HTMLDivElement | null>(null);
  const hint = secao.hint_itens?.length ? secao.hint_itens : secao.itens;
  const showHint = (secao.colapsado || !!secao.hint_itens?.length) && hint.length > 0;

  return (
    <>
      <div
        ref={anchorRef}
        onMouseEnter={() => showHint && setOpen(true)}
        style={{
          display: "flex",
          width: "100%",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 8,
          padding: "8px 10px",
          border: "1px solid var(--border)",
          borderRadius: 8,
          background: open ? "rgba(255,255,255,0.05)" : "rgba(255,255,255,0.03)",
          cursor: showHint ? "help" : "default",
        }}
      >
        <span style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 13, fontWeight: 600 }}>
          {secao.label}
          {secao.editado_humano ? (
            <span style={{ fontSize: 10, fontWeight: 700, color: "var(--accent-copper, #b8722c)", border: "1px solid var(--accent-copper, #b8722c)", borderRadius: 999, padding: "1px 6px" }}>
              editado
            </span>
          ) : null}
        </span>
        <span style={{ fontSize: 13, fontWeight: 600, fontVariantNumeric: "tabular-nums", letterSpacing: "-0.02em", whiteSpace: "nowrap" }}>{formatCurrency(secao.total)}</span>
      </div>
      {showHint && (
        <PortalDropdown open={open} onClose={() => setOpen(false)} anchorRef={anchorRef} minWidth={280}>
          <div
            onMouseEnter={() => setOpen(true)}
            onMouseLeave={() => setOpen(false)}
            style={{
              background: "#131a20",
              border: "1px solid var(--border)",
              borderRadius: 12,
              boxShadow: "0 18px 44px rgba(0,0,0,0.55)",
              padding: 12,
              color: "var(--text)",
            }}
          >
            <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 8 }}>{secao.label}</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 4, maxHeight: 260, overflowY: "auto" }}>
              {hint.map((it, i) => (
                <div key={i} style={{ display: "flex", justifyContent: "space-between", gap: 12, fontSize: 12, padding: "3px 0" }}>
                  <span style={{ color: "var(--muted)" }}>
                    {it.label}
                    {"qtd" in it && it.qtd ? ` · ${Number(it.qtd).toLocaleString("pt-BR", { maximumFractionDigits: 0 })}` : ""}
                  </span>
                  <span style={{ fontVariantNumeric: "tabular-nums", fontWeight: 600 }}>{formatCurrency(it.valor)}</span>
                </div>
              ))}
            </div>
          </div>
        </PortalDropdown>
      )}
    </>
  );
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
  const singleValue = secao.secao === "dinheiro";
  const [open, setOpen] = useState(false);
  const [rows, setRows] = useState<{ descricao: string; valor: string }[]>([]);
  const [saving, setSaving] = useState(false);
  const anchorRef = useRef<HTMLButtonElement | null>(null);

  useEffect(() => {
    if (open) {
      if (singleValue) {
        const first = secao.itens[0];
        setRows([{
          descricao: first?.label || secao.label || "Dinheiro em espécie",
          valor: numberToMask(first?.valor ?? secao.total ?? secao.valor_sistema ?? ""),
        }]);
      } else {
        const base = secao.itens.map((i) => ({
          descricao: i.label,
          valor: numberToMask(i.valor ?? ""),
        }));
        setRows(base.length ? base : [{ descricao: "", valor: "" }]);
      }
    }
  }, [open, secao.itens, secao.label, secao.total, secao.valor_sistema, singleValue]);

  const save = async () => {
    if (!idEmpresa) return;
    setSaving(true);
    try {
      const itens = singleValue
        ? [{
            descricao: (rows[0]?.descricao || secao.label || "Dinheiro em espécie").trim(),
            valor: parseCurrencyMask(rows[0]?.valor ?? ""),
          }]
        : rows
            .filter((r) => r.descricao.trim())
            .map((r) => ({
              descricao: r.descricao.trim(),
              valor: parseCurrencyMask(r.valor),
            }));
      await apiPost(`/bi/profit-management/solvencia/manual${idEmpresa ? `?id_empresa=${idEmpresa}` : ""}`, {
        id_filial: filial,
        ano_mes: anoMes,
        id_tipo: secao.id_tipo,
        itens,
      });
      setOpen(false);
      onSaved();
    } finally {
      setSaving(false);
    }
  };

  const vazio = secao.itens.length === 0;
  const inputStyle = {
    padding: "6px 8px",
    borderRadius: 6,
    border: "1px solid var(--border)",
    background: "#0d1317",
    color: "var(--text)",
    fontSize: 12,
  } as const;

  return (
    <>
      <button
        ref={anchorRef}
        type="button"
        onClick={() => setOpen((o) => !o)}
        title={singleValue ? "Clique para alterar o valor" : "Clique para preencher / editar"}
        style={{
          display: "flex",
          width: "100%",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 8,
          padding: "8px 10px",
          border: "1px dashed var(--border)",
          borderRadius: 8,
          background: open ? "rgba(255,255,255,0.05)" : "transparent",
          color: "inherit",
          cursor: "pointer",
          textAlign: "left",
        }}
      >
        <span style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 13, fontWeight: 500 }}>
          {secao.label}
          <span aria-hidden style={{ fontSize: 11, opacity: 0.7 }}>✎</span>
          {secao.editado_humano ? (
            <span style={{ fontSize: 10, fontWeight: 700, color: "var(--accent-copper, #b8722c)", border: "1px solid var(--accent-copper, #b8722c)", borderRadius: 999, padding: "1px 6px" }}>
              valor editado
            </span>
          ) : null}
        </span>
        <span style={{ fontSize: 14, fontWeight: 600, fontVariantNumeric: "tabular-nums" }}>
          {vazio && !singleValue ? <span style={{ fontSize: 12, opacity: 0.7, fontWeight: 400 }}>clique para preencher</span> : formatCurrency(secao.total)}
        </span>
      </button>

      <PortalDropdown open={open} onClose={() => setOpen(false)} anchorRef={anchorRef} minWidth={singleValue ? 280 : 320}>
        <div
          style={{
            background: "#131a20",
            border: "1px solid var(--border)",
            borderRadius: 12,
            boxShadow: "0 18px 44px rgba(0,0,0,0.55)",
            padding: 12,
            color: "var(--text)",
          }}
        >
          <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 8, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span>{secao.label} — {fmtMonth(anoMes)}</span>
            <button type="button" onClick={() => setOpen(false)} style={{ border: "none", background: "transparent", cursor: "pointer", opacity: 0.6, fontSize: 14, color: "var(--text)" }}>✕</button>
          </div>
          {singleValue ? (
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              <label style={{ fontSize: 11, color: "var(--muted)" }}>Valor (R$)</label>
              <input
                placeholder="0,00"
                inputMode="decimal"
                value={rows[0]?.valor ?? ""}
                onChange={(e) =>
                  setRows([{
                    descricao: rows[0]?.descricao || secao.label,
                    valor: formatCurrencyMask(e.target.value),
                  }])
                }
                style={{ ...inputStyle, width: "100%", textAlign: "right", fontSize: 16, fontWeight: 600, padding: "10px 12px" }}
                autoFocus
              />
              {secao.valor_sistema != null ? (
                <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 2 }}>
                  Valor de sistema: {formatCurrency(secao.valor_sistema)}
                </div>
              ) : null}
            </div>
          ) : (
            <>
              <div style={{ display: "flex", flexDirection: "column", gap: 6, maxHeight: 240, overflowY: "auto" }}>
                {rows.map((r, i) => (
                  <div key={i} style={{ display: "flex", gap: 6 }}>
                    <input
                      placeholder="Nome"
                      value={r.descricao}
                      onChange={(e) => setRows((rs) => rs.map((x, j) => (j === i ? { ...x, descricao: e.target.value } : x)))}
                      style={{ ...inputStyle, flex: 1, minWidth: 0 }}
                    />
                    <input
                      placeholder="0,00"
                      inputMode="decimal"
                      value={r.valor}
                      onChange={(e) =>
                        setRows((rs) =>
                          rs.map((x, j) => (j === i ? { ...x, valor: formatCurrencyMask(e.target.value) } : x))
                        )
                      }
                      style={{ ...inputStyle, width: 120, textAlign: "right" }}
                    />
                    <button type="button" onClick={() => setRows((rs) => rs.filter((_, j) => j !== i))} title="Remover" style={{ border: "none", background: "transparent", cursor: "pointer", opacity: 0.6, fontSize: 14, color: "var(--text)" }}>✕</button>
                  </div>
                ))}
              </div>
              <button
                type="button"
                onClick={() => setRows((rs) => [...rs, { descricao: "", valor: "" }])}
                style={{ marginTop: 8, border: "1px dashed var(--border)", background: "transparent", color: "var(--text)", borderRadius: 6, padding: "6px 8px", cursor: "pointer", fontSize: 12, width: "100%" }}
              >
                + adicionar linha
              </button>
            </>
          )}
          <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 10 }}>
            <button type="button" onClick={() => setOpen(false)} style={{ border: "1px solid var(--border)", background: "transparent", color: "var(--text)", borderRadius: 6, padding: "6px 12px", cursor: "pointer", fontSize: 12 }}>Cancelar</button>
            <button type="button" onClick={save} disabled={saving} style={{ border: "none", background: "var(--color-positive)", color: "#fff", borderRadius: 6, padding: "6px 14px", cursor: "pointer", fontSize: 12, fontWeight: 600, opacity: saving ? 0.6 : 1 }}>{saving ? "Salvando…" : "Salvar"}</button>
          </div>
        </div>
      </PortalDropdown>
    </>
  );
}

function GrupoPanel({ grupoKey, grupo, filial, anoMes, idEmpresa, onSaved }: { grupoKey: string; grupo: Grupo; filial: number; anoMes: number; idEmpresa?: number; onSaved: () => void }) {
  const c = (GRUPO as any)[grupoKey] || GRUPO.ativo_circulante;
  return (
    <div className="card" style={{ padding: 0, overflow: "hidden", borderTop: `3px solid ${c.cor}`, minWidth: 0 }}>
      <div className="solvenciaGrupoHead" style={{ background: c.bg }}>
        <div className="solvenciaGrupoHeadLabel" style={{ color: c.cor }}>{grupo.label}</div>
        <div className="solvenciaGrupoHeadValue" style={{ color: c.cor }}>{formatCurrency(grupo.total)}</div>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 8, padding: 12 }}>
        {grupo.secoes.length === 0 && <div style={{ fontSize: 12, opacity: 0.6 }}>Sem itens.</div>}
        {grupo.secoes
          .filter((secao) => secao.editavel || Math.abs(secao.total) > 0.005 || (secao.hint_itens?.length ?? 0) > 0 || secao.itens.length > 0)
          .map((secao) => {
          if (secao.editavel) {
            return <EditableSecao key={secao.secao} filial={filial} secao={secao} anoMes={anoMes} idEmpresa={idEmpresa} onSaved={onSaved} />;
          }
          const collapsed = !!secao.colapsado || !!secao.hint_itens?.length || secao.itens.length <= 1;
          if (collapsed) {
            return <HintSecao key={secao.secao} secao={secao} />;
          }
          return (
            <div key={secao.secao} style={{ border: "1px solid var(--border)", borderRadius: 8, overflow: "hidden" }}>
              <div style={{ display: "flex", justifyContent: "space-between", padding: "7px 10px", fontSize: 13, fontWeight: 600, background: "rgba(255,255,255,0.03)" }}>
                <span>{secao.label}</span>
                <span style={{ fontVariantNumeric: "tabular-nums" }}>{formatCurrency(secao.total)}</span>
              </div>
              <div style={{ padding: "2px 10px 6px" }}>
                {secao.itens.map((it, i) => (
                  <div key={i} style={{ display: "flex", justifyContent: "space-between", fontSize: 12, padding: "3px 0", color: "var(--muted)" }}>
                    <span>{it.label}{it.qtd ? ` · ${it.qtd.toLocaleString("pt-BR", { maximumFractionDigits: 0 })} L` : ""}</span>
                    <span style={{ fontVariantNumeric: "tabular-nums" }}>{formatCurrency(it.valor)}</span>
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function Kpi({ label, sub, value, color }: { label: string; sub: string; value: string; color: string }) {
  return (
    <div className="solvenciaKpiCard">
      <div className="sectionEyebrow">{label}</div>
      <div className="solvenciaKpiValue" style={{ color }}>{value}</div>
      <div className="solvenciaKpiSub">{sub}</div>
    </div>
  );
}

export function SolvenciaDetalhada({
  data,
  idEmpresa,
  onSaved,
}: {
  data: any;
  idEmpresa?: number;
  onSaved: () => void;
}) {
  if (!data) return null;
  const anoMes: number = Number(data.ano_mes);
  const consideraAnc = Boolean(data.considerar_nao_circulantes);
  const filiais: Filial[] = [...(data.filiais || [])].sort((a, b) =>
    (a.nome || "").localeCompare(b.nome || "", "pt-BR", { numeric: true, sensitivity: "base" }),
  );

  return (
    <div style={{ marginTop: 16 }}>
      <div className="card" style={{ padding: 16 }}>
        <div className="sectionEyebrow">
          Solvência — Abertura do mês{anoMes ? ` · ${fmtMonth(anoMes)}` : ""}
        </div>
        <div style={{ fontSize: 13, color: "var(--muted)", marginTop: 4 }}>
          Posição patrimonial (estoque) na virada do mês. Passe o mouse em Cartões, Cheques e Despesas para o detalhe. Não confundir com o Lucro do DRE (fluxo do período). O mês é o filtro compartilhado no topo (junto de Regime de caixa).
        </div>
      </div>

      {filiais.map((f) => {
        const t = f.totais;
        const liq = t.liquidez_corrente;
        const cobre = t.cobre_passivo;
        return (
          <div
            key={f.id_filial}
            className="solvenciaFilialCard"
            style={{
              borderLeft: `4px solid ${cobre ? "var(--color-positive)" : "var(--color-negative)"}`,
            }}
          >
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 8, marginBottom: 12 }}>
              <div style={{ fontSize: 15, fontWeight: 700 }}>{f.nome}</div>
              <span
                style={{
                  fontSize: 11,
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

            <div className="solvenciaKpiBoard" aria-label="Indicadores de solvência">
              <div className="solvenciaKpiCol">
                <Kpi
                  label="Ativo COM estoque"
                  sub={consideraAnc ? "Circulante + ANC (inclui estoque)" : "Só circulante (inclui estoque)"}
                  value={formatCurrency(t.ativo_com_estoque ?? t.ativo_total)}
                  color="var(--color-positive)"
                />
                <Kpi
                  label="Ativo SEM estoque"
                  sub={consideraAnc ? "Circulante + ANC − estoque" : "Circulante − estoque (loja + combustível)"}
                  value={formatCurrency(t.ativo_sem_estoque ?? t.ativo_total)}
                  color="var(--color-positive)"
                />
              </div>
              <div className="solvenciaKpiCol solvenciaKpiColSolo">
                <Kpi label="Passivo" sub="Contas a pagar + despesas do mês" value={formatCurrency(t.passivo)} color="var(--color-negative)" />
              </div>
              <div className="solvenciaKpiCol">
                <Kpi
                  label="Capital de Giro COM estoque"
                  sub="Ativo COM − Passivo"
                  value={formatCurrency(t.capital_giro_com_estoque ?? t.capital_giro)}
                  color={(t.capital_giro_com_estoque ?? t.capital_giro) >= 0 ? "var(--color-positive)" : "var(--color-negative)"}
                />
                <Kpi
                  label="Capital de Giro SEM estoque"
                  sub="Ativo SEM − Passivo"
                  value={formatCurrency(t.capital_giro_sem_estoque ?? t.capital_giro)}
                  color={(t.capital_giro_sem_estoque ?? t.capital_giro) >= 0 ? "var(--color-positive)" : "var(--color-negative)"}
                />
              </div>
              <div className="solvenciaKpiCol">
                <Kpi
                  label="Liquidez COM estoque"
                  sub="Ativo COM ÷ Passivo"
                  value={liq != null ? `${liq.toFixed(2).replace(".", ",")}×` : "—"}
                  color={(liq ?? 0) >= 1 ? "var(--color-positive)" : "var(--color-negative)"}
                />
                <Kpi
                  label="Liquidez SEM estoque"
                  sub="Ativo SEM ÷ Passivo"
                  value={t.liquidez_sem_estoque != null ? `${Number(t.liquidez_sem_estoque).toFixed(2).replace(".", ",")}×` : "—"}
                  color={(t.liquidez_sem_estoque ?? 0) >= 1 ? "var(--color-positive)" : "var(--color-negative)"}
                />
              </div>
            </div>

            <div className="solvenciaGrupoGrid">
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
        <div className="card" style={{ marginTop: 16, padding: 20, textAlign: "center", color: "var(--muted)" }}>
          Sem dados de solvência para o mês selecionado.
        </div>
      )}
    </div>
  );
}
