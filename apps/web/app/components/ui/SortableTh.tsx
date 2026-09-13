'use client';

import type { KeyboardEvent, ReactNode } from 'react';

type AriaSort = 'none' | 'ascending' | 'descending';

type Props = {
  label: ReactNode;
  sortKey: string;
  ariaSort?: AriaSort;
  onToggle: (key: string) => void;
  align?: 'left' | 'right' | 'center';
  className?: string;
};

/** Cabeçalho ordenável (clique e teclado), sem disparar seleção da linha. */
export default function SortableTh({
  label,
  sortKey,
  ariaSort = 'none',
  onToggle,
  align = 'left',
  className = '',
}: Props) {
  const marker = ariaSort === 'ascending' ? ' ↑' : ariaSort === 'descending' ? ' ↓' : '';
  const onKeyDown = (event: KeyboardEvent<HTMLButtonElement>) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      onToggle(sortKey);
    }
  };
  return (
    <th
      aria-sort={ariaSort}
      className={className}
      style={{ textAlign: align, whiteSpace: 'nowrap' }}
    >
      <button
        type="button"
        className="sortableThBtn"
        onClick={(event) => {
          event.stopPropagation();
          onToggle(sortKey);
        }}
        onKeyDown={onKeyDown}
      >
        {label}
        <span aria-hidden="true">{marker}</span>
      </button>
    </th>
  );
}
