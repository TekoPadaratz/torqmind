'use client';

type Props = {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  'aria-label'?: string;
  className?: string;
};

/** Campo de busca universal dos grids — largura fixa 280px (contrato 08). */
export default function GridSearchInput({
  value,
  onChange,
  placeholder = 'Pesquisar…',
  'aria-label': ariaLabel = 'Pesquisar no grid',
  className = '',
}: Props) {
  return (
    <input
      className={`input gridSearchInput ${className}`.trim()}
      type="search"
      value={value}
      placeholder={placeholder}
      aria-label={ariaLabel}
      onChange={(e) => onChange(e.target.value)}
    />
  );
}
