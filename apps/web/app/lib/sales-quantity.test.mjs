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

test("classifies gasoline, diesel, ethanol and gnv as liters by product or group heuristics", () => {
  const gasolina = {
    produto_nome: "GASOLINA ADITIVADA",
    grupo_nome: "COMBUSTIVEIS",
  };
  const etanol = {
    produto_nome: "ETANOL COMUM",
    grupo_nome: "COMBUSTIVEIS",
  };
  const gnv = {
    produto_nome: "GNV VEICULAR",
    grupo_nome: "Pista",
  };

  assert.equal(classifySalesQuantityKind(gasolina), "fuel");
  assert.equal(classifySalesQuantityKind(etanol), "fuel");
  assert.equal(classifySalesQuantityKind(gnv), "fuel");
  assert.equal(formatSalesQuantity(1500, gasolina), "1.500 L");
  assert.equal(formatSalesQuantity(980.125, etanol), "980,125 L");
  assert.equal(formatSalesQuantity(77.5, gnv), "77,5 L");
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
  assert.equal(formatSalesQuantity(12.5, item), "12,5 un.");
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
