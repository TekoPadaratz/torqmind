const INTEGER_FMT = new Intl.NumberFormat("pt-BR", {
  minimumFractionDigits: 0,
  maximumFractionDigits: 0,
});

const UNIT_FMT = new Intl.NumberFormat("pt-BR", {
  minimumFractionDigits: 0,
  maximumFractionDigits: 2,
});

const FUEL_FMT = new Intl.NumberFormat("pt-BR", {
  minimumFractionDigits: 0,
  maximumFractionDigits: 3,
});

function normalizeText(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[^\w\s]/g, "")
    .toUpperCase()
    .trim();
}

export function classifySalesQuantityKind(item) {
  const explicitKind = String(item?.quantity_kind || "").trim().toLowerCase();
  if (explicitKind === "fuel") return "fuel";
  if (explicitKind === "unit") return "unit";

  const unit = normalizeText(item?.unidade);
  if (["LT", "L", "LITRO", "LITROS"].includes(unit)) return "fuel";

  const groupName = normalizeText(item?.grupo_nome);
  const productName = normalizeText(item?.produto_nome);
  if (
    groupName.includes("COMBUST") ||
    groupName.includes("GNV") ||
    productName.includes("GASOL") ||
    productName.includes("DIESEL") ||
    productName.includes("ETANOL") ||
    productName.includes("ALCOOL") ||
    productName.includes("GNV")
  ) {
    return "fuel";
  }

  return "unit";
}

export function formatSalesQuantity(value, item) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount)) return "-";

  const kind = classifySalesQuantityKind(item);
  if (kind === "fuel") {
    return `${FUEL_FMT.format(amount)} L`;
  }

  if (Math.abs(amount - Math.round(amount)) < 0.0005) {
    return `${INTEGER_FMT.format(Math.round(amount))} un.`;
  }

  return `${UNIT_FMT.format(amount)} un.`;
}
