'use client';

import { useEffect, useMemo, useState } from 'react';

export const GRID_PAGE_SIZE = 30;

type Props = {
  page: number;
  totalPages: number;
  total: number;
  pageSize?: number;
  onPrev: () => void;
  onNext: () => void;
  className?: string;
};

/** Paginação canônica dos grids — 30/página; botões cobre (claro/escuro). */
export default function GridPager({
  page,
  totalPages,
  total,
  pageSize = GRID_PAGE_SIZE,
  onPrev,
  onNext,
  className = '',
}: Props) {
  if (total <= pageSize) return null;
  const from = (page - 1) * pageSize + 1;
  const to = Math.min(page * pageSize, total);
  return (
    <div className={`gridPager ${className}`.trim()}>
      <span className="gridPagerMeta muted">
        {from}–{to} de {total}
      </span>
      <div className="gridPagerActions">
        <button
          type="button"
          className="gridPagerBtn"
          disabled={page <= 1}
          onClick={onPrev}
          aria-label="Página anterior"
        >
          ‹
        </button>
        <span className="gridPagerPage muted">
          {page}/{totalPages}
        </span>
        <button
          type="button"
          className="gridPagerBtn"
          disabled={page >= totalPages}
          onClick={onNext}
          aria-label="Próxima página"
        >
          ›
        </button>
      </div>
    </div>
  );
}

/** Paginação client-side reutilizável (listas já filtradas no FE). */
export function useClientPager<T>(rows: T[], pageSize = GRID_PAGE_SIZE) {
  const [page, setPage] = useState(1);
  const total = rows.length;
  const totalPages = Math.max(1, Math.ceil(total / pageSize) || 1);

  useEffect(() => {
    setPage(1);
  }, [total, pageSize]);

  const safe = Math.min(Math.max(1, page), totalPages);
  const slice = useMemo(
    () => rows.slice((safe - 1) * pageSize, safe * pageSize),
    [rows, safe, pageSize],
  );

  return {
    page: safe,
    totalPages,
    total,
    pageSize,
    slice,
    onPrev: () => setPage((p) => Math.max(1, p - 1)),
    onNext: () => setPage((p) => Math.min(totalPages, p + 1)),
  };
}
