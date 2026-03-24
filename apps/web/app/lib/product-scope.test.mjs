import assert from "node:assert/strict";
import test from "node:test";

import {
  buildProductHref,
  buildScopeSearchParams,
  getScopeControls,
  readScopeFromSearch,
} from "./product-scope.mjs";

test("product scope builder keeps legacy single-branch links when exactly one filial is selected", () => {
  const params = buildScopeSearchParams({
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    dt_ref: "2026-03-24",
    id_empresa: 12,
    id_filiais: [7],
  });

  assert.equal(
    params.toString(),
    "dt_ini=2026-03-01&dt_fim=2026-03-24&id_empresa=12&id_filial=7",
  );
  assert.equal(
    buildProductHref("/dashboard", {
      dt_ini: "2026-03-01",
      dt_fim: "2026-03-24",
      dt_ref: "2026-03-24",
      id_empresa: 12,
      id_filiais: [7],
    }),
    "/dashboard?dt_ini=2026-03-01&dt_fim=2026-03-24&id_empresa=12&id_filial=7",
  );
});

test("product scope builder serializes repeated id_filiais for multi-branch selection", () => {
  const params = buildScopeSearchParams({
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    id_empresa: 12,
    id_filiais: [7, 9, 11],
  });

  assert.equal(
    params.toString(),
    "dt_ini=2026-03-01&dt_fim=2026-03-24&id_empresa=12&id_filiais=7&id_filiais=9&id_filiais=11",
  );
});

test("legacy and repeated branch filters remain readable for compatibility", () => {
  const legacyScope = readScopeFromSearch(
    new URLSearchParams("dt_ini=2026-03-01&dt_fim=2026-03-24&dt_ref=2026-03-24&id_empresa=3&id_filial=9"),
  );
  assert.deepEqual(legacyScope, {
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    dt_ref: "2026-03-24",
    id_empresa: "3",
    id_filial: "9",
    id_filiais: ["9"],
  });

  const multiScope = readScopeFromSearch(
    new URLSearchParams("dt_ini=2026-03-01&dt_fim=2026-03-24&id_empresa=3&id_filiais=9&id_filiais=11"),
  );
  assert.deepEqual(multiScope, {
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    dt_ref: "",
    id_empresa: "3",
    id_filial: null,
    id_filiais: ["9", "11"],
  });
});

test("csv branch fallbacks from auth home_path remain readable", () => {
  const scope = readScopeFromSearch(
    new URLSearchParams("dt_ini=2026-03-01&dt_fim=2026-03-24&id_empresa=3"),
    { id_filiais: "9,11" },
  );

  assert.deepEqual(scope, {
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    dt_ref: "",
    id_empresa: "3",
    id_filial: null,
    id_filiais: ["9", "11"],
  });
});

test("scope controls distinguish platform master, owner and branch manager", () => {
  assert.deepEqual(getScopeControls({ user_role: "platform_master" }), {
    canSwitchCompany: true,
    canSwitchBranch: true,
    canSelectMultipleBranches: true,
    branchLocked: false,
  });

  assert.deepEqual(getScopeControls({ user_role: "tenant_admin" }), {
    canSwitchCompany: false,
    canSwitchBranch: true,
    canSelectMultipleBranches: true,
    branchLocked: false,
  });

  assert.deepEqual(getScopeControls({ user_role: "tenant_manager", id_filial: 5 }), {
    canSwitchCompany: false,
    canSwitchBranch: false,
    canSelectMultipleBranches: false,
    branchLocked: true,
  });
});
