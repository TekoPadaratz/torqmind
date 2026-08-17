import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

import { copyTextToClipboard } from "./copy-to-clipboard.mjs";

test("clipboard API copia a chave", async () => {
  const writes = [];
  const api = {
    navigator: {
      clipboard: {
        async writeText(value) {
          writes.push(value);
        },
      },
    },
  };
  const ok = await copyTextToClipboard("  352601123456789  ", api);
  assert.equal(ok, true);
  assert.deepEqual(writes, ["352601123456789"]);
});

test("HTTP/clipboard recusado cai no execCommand", async () => {
  const created = [];
  const api = {
    navigator: {
      clipboard: {
        async writeText() {
          throw new Error("NotAllowedError");
        },
      },
    },
    document: {
      body: { appendChild(node) { created.push(node); } },
      createElement() {
        return {
          value: "",
          style: {},
          setAttribute() {},
          select() {},
          remove() {},
        };
      },
      execCommand(cmd) {
        return cmd === "copy";
      },
    },
  };
  const ok = await copyTextToClipboard("CHAVE44", api);
  assert.equal(ok, true);
  assert.equal(created.length, 1);
  assert.equal(created[0].value, "CHAVE44");
});

test("string vazia não copia", async () => {
  const api = {
    navigator: { clipboard: { async writeText() { throw new Error("should not run"); } } },
  };
  assert.equal(await copyTextToClipboard("   ", api), false);
});

test("grid ANP copia chave com cursor copy e toast discreto", () => {
  const panel = readFileSync(
    join(import.meta.dirname, "../profit-management/AnpCompliancePanel.tsx"),
    "utf8",
  );
  const css = readFileSync(join(import.meta.dirname, "../globals.css"), "utf8");
  assert.match(panel, /Chave copiada/);
  assert.match(panel, /copyTextToClipboard/);
  assert.match(panel, /className="anpChaveCopy"/);
  assert.match(panel, /const \[draftIni, setDraftIni\]/);
  assert.match(css, /button\.anpChaveCopy[\s\S]*cursor:\s*copy/);
});
