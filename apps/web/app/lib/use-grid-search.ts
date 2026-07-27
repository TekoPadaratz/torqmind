'use client';

import { useMemo, useState } from 'react';
import { normalizeTerm, rowMatchesGridSearch } from './grid-search.mjs';

export { rowMatchesGridSearch } from './grid-search.mjs';

export function useGridSearch<T extends Record<string, unknown>>(
  rows: T[] | null | undefined,
  options?: { excludeKeys?: RegExp; keys?: string[] },
) {
  const [query, setQuery] = useState('');
  const filteredRows = useMemo(() => {
    const list = Array.isArray(rows) ? rows : [];
    if (!normalizeTerm(query)) return list;
    return list.filter((row) => rowMatchesGridSearch(row, query, options));
  }, [rows, query, options?.excludeKeys, options?.keys]);

  return { query, setQuery, filteredRows };
}
