import assert from "node:assert/strict";
import test from "node:test";

import { buildScopeSearchParams } from "./product-scope.mjs";
import {
  getVisibleBranches,
  resolveAppliedBranchIds,
} from "./branch-state.mjs";

const branches = [
  { id_filial: 30, nome: "Zulu Norte" },
  { id_filial: 12, nome: "São José" },
  { id_filial: 7, nome: "Alpha Centro" },
  { id_filial: 22, nome: "Santa Luzia" },
];

test("branch list is sorted alphabetically by nome instead of filial code", () => {
  assert.deepEqual(
    getVisibleBranches(branches).map((branch) => `${branch.nome}:${branch.id_filial}`),
    [
      "Alpha Centro:7",
      "Santa Luzia:22",
      "São José:12",
      "Zulu Norte:30",
    ],
  );
});

test("branch search filters incrementally as the user types", () => {
  assert.deepEqual(
    getVisibleBranches(branches, "sa").map((branch) => branch.nome),
    ["Santa Luzia", "São José"],
  );
  assert.deepEqual(
    getVisibleBranches(branches, "sao").map((branch) => branch.nome),
    ["São José"],
  );
});

test("clearing the branch search restores the full sorted list", () => {
  assert.deepEqual(
    getVisibleBranches(branches, "alpha").map((branch) => branch.nome),
    ["Alpha Centro"],
  );
  assert.deepEqual(
    getVisibleBranches(branches, "").map((branch) => branch.nome),
    ["Alpha Centro", "Santa Luzia", "São José", "Zulu Norte"],
  );
});

test("a single selected branch remains applied even when hidden by the current search", () => {
  const visibleBranchIds = getVisibleBranches(branches, "alpha").map((branch) => String(branch.id_filial));
  assert.equal(visibleBranchIds.includes("12"), false);
  assert.deepEqual(
    resolveAppliedBranchIds({
      selectionMode: "selected",
      selectedBranchIds: ["12"],
    }),
    ["12"],
  );
});

test("multiple selected branches remain applied even when both are hidden by the current search", () => {
  const visibleBranchIds = getVisibleBranches(branches, "alpha").map((branch) => String(branch.id_filial));
  assert.equal(visibleBranchIds.includes("12"), false);
  assert.equal(visibleBranchIds.includes("22"), false);
  assert.deepEqual(
    resolveAppliedBranchIds({
      selectionMode: "selected",
      selectedBranchIds: ["22", "12"],
    }),
    ["12", "22"],
  );
});

test("apply scope keeps its current semantics regardless of the branch search text", () => {
  const visibleBranchIds = getVisibleBranches(branches, "alpha").map((branch) => String(branch.id_filial));
  assert.deepEqual(visibleBranchIds, ["7"]);

  const params = buildScopeSearchParams({
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    id_empresa: 3,
    id_filiais: resolveAppliedBranchIds({
      selectionMode: "all",
      selectedBranchIds: ["12", "22"],
    }),
  });

  assert.equal(params.toString(), "dt_ini=2026-03-01&dt_fim=2026-03-24&id_empresa=3");
});
