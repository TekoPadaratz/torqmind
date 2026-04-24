import test from "node:test";
import assert from "node:assert/strict";

import {
  classifySalesQuantityKind,
  formatSalesQuantity,
} from "./sales-quantity.mjs";

test("formats fuel quantities in liters with useful precision", () => {
  const item = {
    quantity_kind: "fuel",
    produto_nome: "GASOLINA COMUM",
    grupo_nome: "Combustíveis",
    unidade: "LT",
  };

  assert.equal(classifySalesQuantityKind(item), "fuel");
  assert.equal(formatSalesQuantity(31878.919, item), "31.878,919 L");
});

test("formats store items as units without fake thousand decimals", () => {
  const item = {
    quantity_kind: "unit",
    produto_nome: "ÁGUA MINERAL",
    grupo_nome: "Conveniência",
    unidade: "UN",
  };

  assert.equal(classifySalesQuantityKind(item), "unit");
  assert.equal(formatSalesQuantity(35, item), "35 un.");
});

test("falls back to fuel heuristics when the API has not been rebuilt yet", () => {
  const item = {
    produto_nome: "DIESEL S10",
    grupo_nome: "(Sem grupo)",
    unidade: "L",
  };

  assert.equal(classifySalesQuantityKind(item), "fuel");
  assert.equal(formatSalesQuantity(1520.25, item), "1.520,25 L");
});
