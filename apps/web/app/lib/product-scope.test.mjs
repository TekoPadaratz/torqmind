import assert from "node:assert/strict";
import test from "node:test";

import {
  buildProductHref,
  buildScopeKey,
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
    scope_epoch: "epoch-123",
  });

  assert.equal(
    params.toString(),
    "dt_ini=2026-03-01&dt_fim=2026-03-24&id_empresa=12&id_filial=7&dt_ref=2026-03-24&scope_epoch=epoch-123",
  );
  assert.equal(
    buildProductHref("/dashboard", {
      dt_ini: "2026-03-01",
      dt_fim: "2026-03-24",
      dt_ref: "2026-03-24",
      id_empresa: 12,
      id_filiais: [7],
      scope_epoch: "epoch-123",
    }),
    "/dashboard?dt_ini=2026-03-01&dt_fim=2026-03-24&id_empresa=12&id_filial=7&dt_ref=2026-03-24&scope_epoch=epoch-123",
  );
});

test("product scope builder serializes repeated id_filiais for multi-branch selection", () => {
  const params = buildScopeSearchParams({
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    dt_ref: "2026-03-24",
    id_empresa: 12,
    id_filiais: [7, 9, 11],
    scope_epoch: "epoch-456",
  });

  assert.equal(
    params.toString(),
    "dt_ini=2026-03-01&dt_fim=2026-03-24&id_empresa=12&id_filiais=7&id_filiais=9&id_filiais=11&dt_ref=2026-03-24&scope_epoch=epoch-456",
  );
});

test("legacy and repeated branch filters remain readable for compatibility", () => {
  const legacyScope = readScopeFromSearch(
    new URLSearchParams("dt_ini=2026-03-01&dt_fim=2026-03-24&dt_ref=2026-03-24&id_empresa=3&id_filial=9&scope_epoch=epoch-legacy"),
  );
  const legacyScopeKey = buildScopeKey({
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    dt_ref: "2026-03-24",
    id_empresa: "3",
    id_filial: "9",
    id_filiais: ["9"],
  });
  assert.deepEqual(legacyScope, {
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    dt_ref: "2026-03-24",
    id_empresa: "3",
    id_filial: "9",
    id_filiais: ["9"],
    branch_scope: "",
    scope_epoch: "epoch-legacy",
    scope_key: legacyScopeKey,
  });

  const multiScope = readScopeFromSearch(
    new URLSearchParams("dt_ini=2026-03-01&dt_fim=2026-03-24&id_empresa=3&id_filiais=9&id_filiais=11"),
  );
  const multiScopeKey = buildScopeKey({
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    dt_ref: "",
    id_empresa: "3",
    id_filial: null,
    id_filiais: ["9", "11"],
  });
  assert.deepEqual(multiScope, {
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    dt_ref: "",
    id_empresa: "3",
    id_filial: null,
    id_filiais: ["9", "11"],
    branch_scope: "",
    scope_epoch: `legacy:${multiScopeKey}`,
    scope_key: multiScopeKey,
  });
});

test("csv branch fallbacks from auth home_path remain readable", () => {
  const scope = readScopeFromSearch(
    new URLSearchParams("dt_ini=2026-03-01&dt_fim=2026-03-24&id_empresa=3"),
    { id_filiais: "9,11" },
  );
  const scopeKey = buildScopeKey({
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    dt_ref: "",
    id_empresa: "3",
    id_filial: null,
    id_filiais: ["9", "11"],
  });

  assert.deepEqual(scope, {
    dt_ini: "2026-03-01",
    dt_fim: "2026-03-24",
    dt_ref: "",
    id_empresa: "3",
    id_filial: null,
    id_filiais: ["9", "11"],
    branch_scope: "",
    scope_epoch: `legacy:${scopeKey}`,
    scope_key: scopeKey,
  });
});

test("scope parser preserves explicit scope epoch and computes deterministic scope key", () => {
  const scope = readScopeFromSearch(
    new URLSearchParams("dt_ini=2026-03-10&dt_fim=2026-03-20&id_empresa=8&id_filial=14458&scope_epoch=epoch-999"),
  );

  assert.equal(scope.scope_epoch, "epoch-999");
  assert.equal(
    scope.scope_key,
    buildScopeKey({
      dt_ini: "2026-03-10",
      dt_fim: "2026-03-20",
      dt_ref: "",
      id_empresa: "8",
      id_filial: "14458",
      id_filiais: ["14458"],
    }),
  );
});

test("all-branches selection is preserved as sentinel across links", () => {
  const params = buildScopeSearchParams({
    dt_ini: "2026-04-01",
    dt_fim: "2026-04-30",
    dt_ref: "2026-04-30",
    id_empresa: 12,
    branch_scope: "all",
  });
  assert.match(params.toString(), /branch_scope=all/);
  assert.ok(!params.toString().includes("id_filial="));
  assert.ok(!params.toString().includes("id_filiais="));

  const parsed = readScopeFromSearch(new URLSearchParams(params.toString()));
  assert.equal(parsed.branch_scope, "all");
  assert.deepEqual(parsed.id_filiais, []);
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

test("channel admin can switch company and branch inside its carteira", () => {
  assert.deepEqual(getScopeControls({ user_role: "channel_admin" }), {
    canSwitchCompany: true,
    canSwitchBranch: true,
    canSelectMultipleBranches: true,
    branchLocked: false,
  });
});
