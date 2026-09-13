'use client';

import GridPager, { GRID_PAGE_SIZE } from './GridPager';

type Props = {
  page: number;
  totalPages: number;
  total: number;
  pageSize?: number;
  from?: number;
  to?: number;
  onPrev: () => void;
  onNext: () => void;
  onResetOrder: () => void;
  isDefaultOrder?: boolean;
  truncatedNote?: string | null;
};

/** Faixa canônica: Exibindo X–Y de Z · Ordem padrão · paginação. */
export default function GridChrome({
  page,
  totalPages,
  total,
  pageSize = GRID_PAGE_SIZE,
  from,
  to,
  onPrev,
  onNext,
  onResetOrder,
  isDefaultOrder = true,
  truncatedNote = null,
}: Props) {
  if (total <= 0) return null;
  const start = from ?? (total === 0 ? 0 : (page - 1) * pageSize + 1);
  const end = to ?? Math.min(page * pageSize, total);
  return (
    <div className="gridChrome">
      <div className="gridChromeMeta">
        <span className="muted">
          Exibindo {start}–{end} de {total.toLocaleString('pt-BR')} registros
        </span>
        {truncatedNote ? <span className="muted">{truncatedNote}</span> : null}
      </div>
      <div className="gridChromeActions">
        <button
          type="button"
          className="gridOrderBtn"
          onClick={onResetOrder}
          disabled={isDefaultOrder}
        >
          Ordem padrão
        </button>
        <GridPager
          page={page}
          totalPages={totalPages}
          total={total}
          pageSize={pageSize}
          onPrev={onPrev}
          onNext={onNext}
          hideMeta
        />
      </div>
    </div>
  );
}
