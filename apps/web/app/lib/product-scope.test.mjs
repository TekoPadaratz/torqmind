import assert from "node:assert/strict";
import test from "node:test";

import {
  buildCanonicalProductHref,
  buildProductHref,
  buildScopeKey,
  buildScopeSearchParams,
  getScopeControls,
  hasExplicitBranchSelection,
  needsCanonicalScope,
  readScopeFromSearch,
} from "./product-scope.mjs";

function assertHrefWithSameParams(actualHref, expectedHref) {
  const actualUrl = new URL(actualHref, "https://torqmind.local");
  const expectedUrl = new URL(expectedHref, "https://torqmind.local");
  const normalize = (url) => {
    const grouped = {};
    for (const [key, value] of url.searchParams.entries()) {
      if (!grouped[key]) {
        grouped[key] = [];
      }
      grouped[key].push(value);
    }
    for (const key of Object.keys(grouped)) {
      grouped[key].sort();
    }
    return grouped;
  };

  assert.equal(actualUrl.pathname, expectedUrl.pathname);
  assert.deepEqual(normalize(actualUrl), normalize(expectedUrl));
}

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
    id_filiais: [7, 9, 11],
    branch_scope: "all",
  });
  assert.match(params.toString(), /branch_scope=all/);
  assert.ok(!params.toString().includes("id_filial="));
  assert.ok(!params.toString().includes("id_filiais="));

  const parsed = readScopeFromSearch(new URLSearchParams(params.toString()));
  assert.equal(parsed.branch_scope, "all");
  assert.deepEqual(parsed.id_filiais, []);
});

test("explicit all-branches URL is not expanded from session fallback", () => {
  const scope = readScopeFromSearch(
    new URLSearchParams("dt_ini=2026-05-01&dt_fim=2026-05-11&id_empresa=1&branch_scope=all&scope_epoch=epoch-all"),
    {
      id_empresa: 1,
      id_filiais: ["11", "13", "17", "19", "23"],
      branch_scope: "all",
      dt_ref: "2026-05-11",
    },
  );

  assert.equal(scope.branch_scope, "all");
  assert.deepEqual(scope.id_filiais, []);
  assert.equal(scope.id_filial, null);
});

test("explicit single-branch URL is not overwritten by broader session fallback", () => {
  const href = buildCanonicalProductHref(
    "/cash?tab=live&dt_ini=2026-05-01&dt_fim=2026-05-11&id_empresa=1&id_filial=14458&scope_epoch=epoch-branch",
    {
      id_empresa: 1,
      accesses: [
        { id_empresa: 1, id_filial: 10169 },
        { id_empresa: 1, id_filial: 14458 },
        { id_empresa: 1, id_filial: 18777 },
      ],
      default_scope: { id_empresa: 1, id_filiais: [10169, 14458, 18777], branch_scope: "all", days: 30 },
    },
  );

  assertHrefWithSameParams(
    href,
    "/cash?tab=live&dt_ini=2026-05-01&dt_fim=2026-05-11&id_empresa=1&id_filial=14458&scope_epoch=epoch-branch&dt_ref=2026-05-11",
  );
});

test("explicit multi-branch URL is not overwritten by broader session fallback", () => {
  const href = buildCanonicalProductHref(
    "/customers?dt_ini=2026-05-01&dt_fim=2026-05-11&id_empresa=1&id_filiais=10169&id_filiais=14458&scope_epoch=epoch-multi",
    {
      id_empresa: 1,
      accesses: [
        { id_empresa: 1, id_filial: 10169 },
        { id_empresa: 1, id_filial: 14458 },
        { id_empresa: 1, id_filial: 18777 },
        { id_empresa: 1, id_filial: 28888 },
      ],
      default_scope: { id_empresa: 1, id_filiais: [10169, 14458, 18777, 28888], branch_scope: "all", days: 30 },
    },
  );

  assertHrefWithSameParams(
    href,
    "/customers?dt_ini=2026-05-01&dt_fim=2026-05-11&id_empresa=1&id_filiais=10169&id_filiais=14458&scope_epoch=epoch-multi&dt_ref=2026-05-11",
  );
});

test("canonical product href keeps explicit all-branches sentinel without appending all fallback branches", () => {
  const href = buildCanonicalProductHref(
    "/finance?dt_ini=2026-05-01&dt_fim=2026-05-11&id_empresa=1&branch_scope=all&scope_epoch=epoch-all",
    {
      id_empresa: 1,
      accesses: [
        { id_empresa: 1, id_filial: 10169 },
        { id_empresa: 1, id_filial: 14458 },
        { id_empresa: 1, id_filial: 18777 },
      ],
      default_scope: { id_empresa: 1, id_filiais: [10169, 14458, 18777], branch_scope: "all", days: 30 },
    },
  );

  assertHrefWithSameParams(
    href,
    "/finance?dt_ini=2026-05-01&dt_fim=2026-05-11&id_empresa=1&branch_scope=all&scope_epoch=epoch-all&dt_ref=2026-05-11",
  );
});

test("explicit branch selection detector distinguishes explicit URL scope from session fallback", () => {
  assert.equal(hasExplicitBranchSelection(new URLSearchParams("branch_scope=all")), true);
  assert.equal(hasExplicitBranchSelection(new URLSearchParams("id_filial=14458")), true);
  assert.equal(hasExplicitBranchSelection(new URLSearchParams("id_filiais=10169&id_filiais=14458")), true);
  assert.equal(hasExplicitBranchSelection(new URLSearchParams("dt_ini=2026-05-01&dt_fim=2026-05-11&id_empresa=1")), false);
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

test("canonical product href builds scoped dashboard link from session fallback", () => {
  const href = buildCanonicalProductHref(
    "/dashboard",
    {
      id_empresa: 12,
      id_filial: 7,
      default_scope: { days: 7 },
    },
    { scopeEpoch: "epoch-login" },
  );

  assert.match(href, /^\/dashboard\?/);
  assert.match(href, /dt_ini=\d{4}-\d{2}-\d{2}/);
  assert.match(href, /dt_fim=\d{4}-\d{2}-\d{2}/);
  assert.match(href, /id_empresa=12/);
  assert.match(href, /id_filial=7/);
  assert.match(href, /scope_epoch=epoch-login/);
});

test("canonical product href derives all accessible branches for company-level sessions", () => {
  const href = buildCanonicalProductHref(
    "/dashboard",
    {
      id_empresa: 12,
      id_filial: null,
      accesses: [
        { id_empresa: 12, id_filial: 7 },
        { id_empresa: 12, id_filial: 9 },
        { id_empresa: 12, id_filial: null },
      ],
      default_scope: { id_empresa: 12, days: 7 },
    },
    { scopeEpoch: "epoch-all-branches" },
  );

  assert.match(href, /^\/dashboard\?/);
  assert.match(href, /id_empresa=12/);
  assert.match(href, /branch_scope=all/);
  assert.doesNotMatch(href, /id_filiais=/);
  assert.match(href, /scope_epoch=epoch-all-branches/);
});

test("all-branches sentinel remains canonical even when full branch list is present in memory", () => {
  const href = buildProductHref("/cash", {
    dt_ini: "2026-05-01",
    dt_fim: "2026-05-11",
    dt_ref: "2026-05-11",
    id_empresa: 1,
    id_filiais: [10169, 14458, 18777],
    branch_scope: "all",
    scope_epoch: "epoch-all-memory",
  });

  assert.equal(
    href,
    "/cash?dt_ini=2026-05-01&dt_fim=2026-05-11&id_empresa=1&branch_scope=all&dt_ref=2026-05-11&scope_epoch=epoch-all-memory",
  );
});

test("canonical product href preserves explicit scope and keeps unrelated params", () => {
  const href = buildCanonicalProductHref(
    "/cash?tab=live&dt_ini=2026-05-01&dt_fim=2026-05-11&id_empresa=1&id_filial=14458",
    {
      id_empresa: 1,
      id_filial: 14458,
      default_scope: { days: 30 },
    },
    { scopeEpoch: "epoch-canonical" },
  );

  assert.equal(
    href,
    "/cash?tab=live&dt_ini=2026-05-01&dt_fim=2026-05-11&id_empresa=1&id_filial=14458&dt_ref=2026-05-11&scope_epoch=epoch-canonical",
  );
});

test("scope canonicalization detects missing URL scope", () => {
  assert.equal(needsCanonicalScope("/dashboard"), true);
  assert.equal(
    needsCanonicalScope("/dashboard?dt_ini=2026-05-01&dt_fim=2026-05-11&id_empresa=1&id_filial=7&scope_epoch=epoch-1"),
    false,
  );
});
