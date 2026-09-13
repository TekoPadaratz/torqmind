'use client';

import { useEffect, useMemo, useRef, useState } from 'react';

import {
  RECORD_PAGE_SIZE,
  clampPage,
  nextSortDir,
  pageRange,
  paginateRows,
  sortRecordRows,
  sumFields,
} from './record-grid.mjs';

export type RecordSortDir = 'asc' | 'desc';
export type RecordColumnType = 'text' | 'number' | 'date';

export type RecordSortSpec<T> = {
  key: string;
  type?: RecordColumnType;
  dir: RecordSortDir;
  getValue?: (row: T) => unknown;
  getTieId?: (row: T, idx: number) => string | number;
};

type Args<T> = {
  rows: T[];
  defaultCompare?: (a: T, b: T) => number;
  defaultSort?: Omit<RecordSortSpec<T>, 'getTieId'> | null;
  getTieId?: (row: T, idx: number) => string | number;
  summableKeys?: string[];
  pageSize?: number;
  resetKey?: string | number;
  columns?: Record<string, { type?: RecordColumnType; getValue?: (row: T) => unknown }>;
};

export function useRecordGrid<T>({
  rows,
  defaultCompare,
  defaultSort = null,
  getTieId,
  summableKeys = [],
  pageSize = RECORD_PAGE_SIZE,
  resetKey,
  columns = {},
}: Args<T>) {
  const [page, setPage] = useState(1);
  const [sort, setSort] = useState<RecordSortSpec<T> | null>(defaultSort);
  const lastReset = useRef(resetKey);

  useEffect(() => {
    if (lastReset.current !== resetKey) {
      lastReset.current = resetKey;
      setPage(1);
    }
  }, [resetKey]);

  useEffect(() => {
    setPage(1);
  }, [rows.length, pageSize, sort?.key, sort?.dir]);

  const ordered = useMemo(() => {
    if (sort?.key) {
      return sortRecordRows(rows, {
        ...sort,
        type: sort.type || columns[sort.key]?.type || 'text',
        getValue: sort.getValue || columns[sort.key]?.getValue,
        getTieId,
      });
    }
    if (defaultCompare) {
      return [...rows].sort((a, b) => {
        const cmp = defaultCompare(a, b);
        if (cmp !== 0) return cmp;
        if (!getTieId) return 0;
        const ia = getTieId(a, 0);
        const ib = getTieId(b, 0);
        if (typeof ia === 'number' && typeof ib === 'number') return ia - ib;
        return String(ia ?? '').localeCompare(String(ib ?? ''), 'pt-BR');
      });
    }
    return rows;
  }, [rows, sort, defaultCompare, getTieId, columns]);

  const pager = useMemo(
    () => paginateRows(ordered, page, pageSize),
    [ordered, page, pageSize],
  );
  const safePage = clampPage(page, pager.total, pageSize);
  const range = pageRange(safePage, pageSize, pager.total);
  const pageTotals = useMemo(
    () => sumFields(pager.slice, summableKeys),
    [pager.slice, summableKeys],
  );
  const filteredTotals = useMemo(
    () => sumFields(ordered, summableKeys),
    [ordered, summableKeys],
  );

  return {
    page: safePage,
    totalPages: pager.totalPages,
    total: pager.total,
    pageSize,
    slice: pager.slice,
    range,
    sort,
    pageTotals,
    filteredTotals,
    isDefaultOrder: sort == null || (defaultSort != null && sort.key === defaultSort.key && sort.dir === defaultSort.dir),
    ariaSort: (key: string): 'none' | 'ascending' | 'descending' => {
      if (!sort || sort.key !== key) return 'none';
      return sort.dir === 'desc' ? 'descending' : 'ascending';
    },
    toggleSort: (key: string) => {
      const type = columns[key]?.type || 'text';
      const defaultDir = type === 'number' || type === 'date' ? 'desc' : 'asc';
      setSort((cur) => ({
        key,
        type,
        dir: nextSortDir(cur?.key, cur?.dir, key, defaultDir) as RecordSortDir,
        getValue: columns[key]?.getValue,
        getTieId,
      }));
      setPage(1);
    },
    resetOrder: () => {
      setSort(defaultSort);
      setPage(1);
    },
    onPrev: () => setPage((p) => Math.max(1, p - 1)),
    onNext: () => setPage((p) => Math.min(pager.totalPages, p + 1)),
    setPage,
  };
}

export { RECORD_PAGE_SIZE };
