'use client';

import { useMemo, useState } from 'react';

const DEFAULT_EXCLUDE = /^(id_.*|.*_id|token|password|hash|payload|raw|reasons|motivos_raw)$/i;

function normalizeTerm(value: string): string {
  return value.trim().toLocaleLowerCase('pt-BR');
}

function stringifyCell(value: unknown): string {
  if (value == null || value === '') return '';
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) {
    return value.map(stringifyCell).filter(Boolean).join(' ');
  }
  if (typeof value === 'object') {
    try {
      return Object.values(value as Record<string, unknown>)
        .map(stringifyCell)
        .filter(Boolean)
        .join(' ');
    } catch {
      return '';
    }
  }
  return '';
}

export function rowMatchesGridSearch(
  row: Record<string, unknown>,
  term: string,
  options?: { excludeKeys?: RegExp; keys?: string[] },
): boolean {
  const needle = normalizeTerm(term);
  if (!needle) return true;
  const exclude = options?.excludeKeys || DEFAULT_EXCLUDE;
  const keys = options?.keys || Object.keys(row || {});
  for (const key of keys) {
    if (exclude.test(key)) continue;
    const hay = normalizeTerm(stringifyCell(row[key]));
    if (hay && hay.includes(needle)) return true;
  }
  return false;
}

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
