function normalizeBranchSearchText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase();
}

export function uniqueBranchIds(values) {
  return [...new Set((values || [])
    .map((value) => String(value ?? "").trim())
    .filter((value) => /^\d+$/.test(value) && Number(value) > 0))]
    .sort((left, right) => Number(left) - Number(right));
}

export function getVisibleBranches(branches, searchText = "") {
  const normalizedSearch = normalizeBranchSearchText(searchText);
  const sortedBranches = [...(branches || [])].sort((left, right) => {
    const leftName = String(left?.nome || "");
    const rightName = String(right?.nome || "");
    const byName = leftName.localeCompare(rightName, "pt-BR", { sensitivity: "base" });
    if (byName !== 0) return byName;
    return Number(left?.id_filial || 0) - Number(right?.id_filial || 0);
  });

  if (!normalizedSearch) return sortedBranches;

  return sortedBranches.filter((branch) =>
    normalizeBranchSearchText(branch?.nome).includes(normalizedSearch),
  );
}

export function resolveAppliedBranchIds({
  branchLocked = false,
  sessionBranchId = null,
  selectionMode = "all",
  selectedBranchIds = [],
} = {}) {
  if (branchLocked) return uniqueBranchIds([sessionBranchId]);
  if (selectionMode === "all") return [];
  return uniqueBranchIds(selectedBranchIds);
}
