import test from "node:test";
import assert from "node:assert/strict";
import { formatGroupLabel, formatSelectedGroupCodes } from "./commission-group-labels.mjs";

test("formatGroupLabel prefixa id_grupo_produto antes do nome", () => {
  assert.equal(formatGroupLabel(5, "LUBRIFICANTES"), "5 — LUBRIFICANTES");
  assert.equal(formatGroupLabel(16, ""), "16 — Grupo 16");
});

test("formatSelectedGroupCodes lista só ids selecionados", () => {
  const groups = [
    { id_grupo_produto: 11, selected: true },
    { id_grupo_produto: 5, selected: true },
    { id_grupo_produto: 3, selected: false },
  ];
  assert.equal(formatSelectedGroupCodes(groups), "5, 11");
  assert.equal(formatSelectedGroupCodes([]), "nenhum");
});
