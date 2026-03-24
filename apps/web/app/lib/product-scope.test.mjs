import assert from "node:assert/strict";
import test from "node:test";

import {
  buildProductHref,
  buildScopeSearchParams,
  getScopeControls,
  readScopeFromSearch,
} from "./product-scope.mjs";

test("product scope builder keeps production filters and omits dt_ref by default", () => {
  const params = buildScopeSearchParams({
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    dt_ref: "2026-03-24",
    id_empresa: 12,
    id_filial: 7,
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
      id_filial: 7,
    }),
    "/dashboard?dt_ini=2026-03-01&dt_fim=2026-03-24&id_empresa=12&id_filial=7",
  );
});

test("legacy dt_ref links remain readable for compatibility", () => {
  const scope = readScopeFromSearch(
    new URLSearchParams("dt_ini=2026-03-01&dt_fim=2026-03-24&dt_ref=2026-03-24&id_empresa=3&id_filial=9"),
  );

  assert.deepEqual(scope, {
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    dt_ref: "2026-03-24",
    id_empresa: "3",
    id_filial: "9",
  });
});

test("scope controls distinguish platform master, owner and branch manager", () => {
  assert.deepEqual(getScopeControls({ user_role: "platform_master" }), {
    canSwitchCompany: true,
    canSwitchBranch: true,
    branchLocked: false,
  });

  assert.deepEqual(getScopeControls({ user_role: "tenant_admin" }), {
    canSwitchCompany: false,
    canSwitchBranch: true,
    branchLocked: false,
  });

  assert.deepEqual(getScopeControls({ user_role: "tenant_manager", id_filial: 5 }), {
    canSwitchCompany: false,
    canSwitchBranch: false,
    branchLocked: true,
  });
});
