"use client";

type Props = {
  value: boolean;
  onChange: (next: boolean) => void;
  disabled?: boolean;
  visible?: boolean;
};

/** Toggle padrão Metas/Comissões (mesmo visual de «Imprimir valores?»). */
export default function CommissionCentralMirrorToggle({
  value,
  onChange,
  disabled,
  visible = true,
}: Props) {
  if (!visible) return null;
  return (
    <button
      type="button"
      className={`profitScopeToggle${value ? " on" : ""}`}
      aria-pressed={value}
      disabled={disabled}
      onClick={() => onChange(!value)}
      title="Incluir vendas espelhadas da filial Central (paridade Xpert LSC)"
    >
      <span className="profitScopeToggleDot" aria-hidden />
      Considera valores da central?
    </button>
  );
}
