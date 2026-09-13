"use client";

import { useMemo } from "react";
import EmptyState from "../components/ui/EmptyState";
import GridChrome from "../components/ui/GridChrome";
import GridSearchInput from "../components/ui/GridSearchInput";
import SortableTh from "../components/ui/SortableTh";
import { formatCurrency, formatDateOnly } from "../lib/format";
import { useGridSearch } from "../lib/use-grid-search";
import { useRecordGrid } from "../lib/use-record-grid";
import { compareGridRows, sortGridRows } from "../lib/grid-sort";

export type DrilldownGroup = {
  id_grupo_produto: number;
  nome: string;
  valor: number;
};

export type DrilldownNote = {
  id_filial?: number;
  id_comprovante?: number;
  data: string;
  data_key?: number;
  documento: string;
  valor: number;
};

export type DrilldownPayload = {
  filial_label?: string;
  venda_bruta_total?: number;
  grupos?: DrilldownGroup[];
  grupos_total?: number;
  perdas_notas?: DrilldownNote[];
  perdas_notas_total?: number;
  perdas_estoque?: number;
  perdas_divergente?: boolean;
};

type Props = {
  payload: DrilldownPayload | null;
  loading: boolean;
};

export default function ManagerCommissionDrilldown({ payload, loading }: Props) {
  const groups = useMemo(() => {
    const list = (payload?.grupos || []).filter((g) => Math.abs(Number(g.valor || 0)) > 0.009);
    list.sort((a, b) => String(a.nome || "").localeCompare(String(b.nome || ""), "pt-BR"));
    return list;
  }, [payload]);

  const notes = useMemo(
    () =>
      sortGridRows(payload?.perdas_notas || [], (row) => ({
        data: row.data || row.data_key,
        nome: row.documento,
      })),
    [payload],
  );

  const {
    query: groupQuery,
    setQuery: setGroupQuery,
    filteredRows: filteredGroups,
  } = useGridSearch(groups as unknown as Record<string, unknown>[], {
    excludeKeys: /^id_/i,
  });

  const {
    query: noteQuery,
    setQuery: setNoteQuery,
    filteredRows: filteredNotes,
  } = useGridSearch(notes as unknown as Record<string, unknown>[], {
    excludeKeys: /^id_/i,
  });

  const groupList = filteredGroups as unknown as DrilldownGroup[];
  const noteList = filteredNotes as unknown as DrilldownNote[];
  const groupsGrid = useRecordGrid<DrilldownGroup>({
    rows: groupList,
    resetKey: groupQuery,
    getTieId: (row) => row.id_grupo_produto,
    defaultCompare: (a, b) =>
      compareGridRows({ nome: a.nome }, { nome: b.nome }),
    summableKeys: ["valor"],
    columns: { nome: { type: "text" }, valor: { type: "number" } },
  });
  const notesGrid = useRecordGrid<DrilldownNote>({
    rows: noteList,
    resetKey: noteQuery,
    getTieId: (row) => `${row.id_comprovante}-${row.documento}`,
    defaultCompare: (a, b) =>
      compareGridRows(
        { data: a.data || a.data_key, nome: a.documento },
        { data: b.data || b.data_key, nome: b.documento },
      ),
    summableKeys: ["valor"],
    columns: {
      data: { type: "date", getValue: (r) => r.data || r.data_key },
      documento: { type: "text" },
      valor: { type: "number" },
    },
  });

  if (loading && !payload) {
    return <div className="muted" style={{ padding: "8px 4px", fontSize: 12 }}>Carregando detalhe…</div>;
  }
  if (!payload) {
    return <div className="muted" style={{ padding: "8px 4px", fontSize: 12 }}>Sem detalhe para esta filial.</div>;
  }

  const notasAllTotal = Number(payload.perdas_notas_total || 0);

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <section>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 12,
            flexWrap: "wrap",
            marginBottom: 8,
          }}
        >
          <h3 style={{ margin: 0, fontSize: 14, fontWeight: 700 }}>Grupos</h3>
          <GridSearchInput value={groupQuery} onChange={setGroupQuery} />
        </div>
        {groupList.length === 0 ? (
          <EmptyState title="Sem grupos" detail="Nenhum grupo com valor no período." />
        ) : (
          <>
          <div className="tableScroll">
            <table className="table compact" style={{ width: "100%", minWidth: 360 }}>
              <thead>
                <tr>
                  <SortableTh label="Grupo" sortKey="nome" ariaSort={groupsGrid.ariaSort("nome")} onToggle={groupsGrid.toggleSort} />
                  <SortableTh label="Valor" sortKey="valor" ariaSort={groupsGrid.ariaSort("valor")} onToggle={groupsGrid.toggleSort} align="right" />
                </tr>
              </thead>
              <tbody>
                {groupsGrid.slice.map((g) => (
                  <tr key={g.id_grupo_produto}>
                    <td style={{ textAlign: "left" }}>{g.nome || "—"}</td>
                    <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                      {formatCurrency(g.valor)}
                    </td>
                  </tr>
                ))}
              </tbody>
              <tfoot className="commissionGridFoot">
                <tr>
                  <td style={{ textAlign: "left", fontWeight: 700 }}>Total da página ({groupsGrid.slice.length})</td>
                  <td style={{ textAlign: "right", fontWeight: 700, fontVariantNumeric: "tabular-nums" }}>
                    {formatCurrency(groupsGrid.pageTotals.valor)}
                  </td>
                </tr>
              </tfoot>
            </table>
          </div>
          <GridChrome
            page={groupsGrid.page}
            totalPages={groupsGrid.totalPages}
            total={groupsGrid.total}
            from={groupsGrid.range.from}
            to={groupsGrid.range.to}
            onPrev={groupsGrid.onPrev}
            onNext={groupsGrid.onNext}
            onResetOrder={groupsGrid.resetOrder}
            isDefaultOrder={groupsGrid.isDefaultOrder}
          />
          </>
        )}
      </section>

      <section>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 12,
            flexWrap: "wrap",
            marginBottom: 8,
          }}
        >
          <h3 style={{ margin: 0, fontSize: 14, fontWeight: 700 }}>Perdas</h3>
          <GridSearchInput value={noteQuery} onChange={setNoteQuery} />
        </div>
        {payload.perdas_divergente ? (
          <div className="muted" style={{ marginBottom: 8, fontSize: 12 }}>
            Valor na linha {formatCurrency(payload.perdas_estoque)} · notas {formatCurrency(notasAllTotal)}
          </div>
        ) : null}
        {noteList.length === 0 ? (
          <EmptyState title="Sem notas" detail="Nenhuma nota de perda no período." />
        ) : (
          <>
          <div className="tableScroll">
            <table className="table compact" style={{ width: "100%", minWidth: 420 }}>
              <thead>
                <tr>
                  <SortableTh label="Data" sortKey="data" ariaSort={notesGrid.ariaSort("data")} onToggle={notesGrid.toggleSort} />
                  <SortableTh label="Documento" sortKey="documento" ariaSort={notesGrid.ariaSort("documento")} onToggle={notesGrid.toggleSort} />
                  <SortableTh label="Valor" sortKey="valor" ariaSort={notesGrid.ariaSort("valor")} onToggle={notesGrid.toggleSort} align="right" />
                </tr>
              </thead>
              <tbody>
                {notesGrid.slice.map((n) => (
                  <tr key={`${n.id_comprovante}-${n.documento}`}>
                    <td style={{ textAlign: "left", whiteSpace: "nowrap" }}>
                      {formatDateOnly(n.data || n.data_key) === "-"
                        ? "—"
                        : formatDateOnly(n.data || n.data_key)}
                    </td>
                    <td style={{ textAlign: "left", fontVariantNumeric: "tabular-nums" }}>
                      {n.documento || "—"}
                    </td>
                    <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                      {formatCurrency(n.valor)}
                    </td>
                  </tr>
                ))}
              </tbody>
              <tfoot className="commissionGridFoot">
                <tr>
                  <td colSpan={2} style={{ textAlign: "left", fontWeight: 700 }}>Total da página ({notesGrid.slice.length})</td>
                  <td style={{ textAlign: "right", fontWeight: 700, fontVariantNumeric: "tabular-nums" }}>
                    {formatCurrency(notesGrid.pageTotals.valor)}
                  </td>
                </tr>
              </tfoot>
            </table>
          </div>
          <GridChrome
            page={notesGrid.page}
            totalPages={notesGrid.totalPages}
            total={notesGrid.total}
            from={notesGrid.range.from}
            to={notesGrid.range.to}
            onPrev={notesGrid.onPrev}
            onNext={notesGrid.onNext}
            onResetOrder={notesGrid.resetOrder}
            isDefaultOrder={notesGrid.isDefaultOrder}
          />
          </>
        )}
      </section>
    </div>
  );
}
