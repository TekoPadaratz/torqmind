'use client';

export type PresetFilterOption = {
  id: string;
  label: string;
};

type Props = {
  options: PresetFilterOption[];
  value: string | null;
  onChange: (id: string | null) => void;
  className?: string;
  allowClear?: boolean;
  clearLabel?: string;
};

/** Chips de filtro pré-programado (cobre ativo) — padrão ouro TorqMind. */
export default function PresetFilterChips({
  options,
  value,
  onChange,
  className = '',
  allowClear = true,
  clearLabel = 'Todos',
}: Props) {
  return (
    <div className={`presetFilterChips ${className}`.trim()} role="group" aria-label="Filtros rápidos">
      {allowClear ? (
        <button
          type="button"
          className={`presetFilterChip${!value ? ' is-active' : ''}`}
          onClick={() => onChange(null)}
        >
          {clearLabel}
        </button>
      ) : null}
      {options.map((opt) => (
        <button
          key={opt.id}
          type="button"
          className={`presetFilterChip${value === opt.id ? ' is-active' : ''}`}
          onClick={() => onChange(value === opt.id ? null : opt.id)}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}
