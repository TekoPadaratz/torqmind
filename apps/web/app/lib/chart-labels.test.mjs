import assert from "node:assert/strict";
import { test } from "node:test";

import {
  ellipsizeLabel,
  honestPersonName,
  shortCategoryLabels,
  shortPersonLabels,
} from "./chart-labels.mjs";

test("honestPersonName preserves API fallback and rejects empty/numeric ids", () => {
  assert.equal(honestPersonName("  LUIZ MICHAEL  "), "LUIZ MICHAEL");
  assert.equal(honestPersonName("Funcionário #1079"), "Funcionário #1079");
  assert.equal(honestPersonName("1079", "Funcionário #1079"), "Funcionário #1079");
  assert.equal(honestPersonName("", "Funcionário #3"), "Funcionário #3");
  assert.equal(honestPersonName(null), "Nome não cadastrado");
});

test("shortPersonLabels uses first name and disambiguates collisions", () => {
  const labels = shortPersonLabels([
    { id: 1, name: "LUIZ MICHAEL DE OLIVEIRA" },
    { id: 2, name: "MARCELO MACHADO DE SOUZA" },
    { id: 3, name: "LUIZ CARLOS DA SILVA" },
    { id: 4, name: "Funcionário #1079" },
  ]);
  assert.equal(labels.get("2"), "MARCELO");
  assert.equal(labels.get("1"), "LUIZ MICHAEL");
  assert.equal(labels.get("3"), "LUIZ CARLOS");
  assert.equal(labels.get("4"), "Funcionário #1079");
});

test("shortPersonLabels never uses the short name as identity", () => {
  const labels = shortPersonLabels([
    { id: 10, name: "JOAO SILVA" },
    { id: 11, name: "JOAO SILVA" },
  ]);
  assert.equal(labels.get("10"), "JOAO");
  assert.equal(labels.get("11"), "JOAO");
  assert.ok(labels.has("10") && labels.has("11"));
});

test("shortCategoryLabels keeps essential distinction and expands collisions", () => {
  const labels = shortCategoryLabels(
    [
      { id: "a", name: "GASOLINA COMUM L" },
      { id: "b", name: "GASOLINA ADITIVADA L" },
    ],
    10,
  );
  assert.notEqual(labels.get("a"), labels.get("b"));
  assert.match(labels.get("a"), /GASOLINA/i);
  assert.match(labels.get("b"), /GASOLINA/i);
});

test("ellipsizeLabel is honest for empty category", () => {
  assert.equal(ellipsizeLabel(""), "—");
  assert.equal(ellipsizeLabel("PIX"), "PIX");
  assert.equal(ellipsizeLabel("CARTAO DE CREDITO VISA", 10), "CARTAO DE…");
});
