/**
 * Copia texto para a área de transferência.
 * Tenta Clipboard API e cai em execCommand (HTTP / contexto inseguro).
 *
 * @param {string} value
 * @param {{ navigator?: { clipboard?: { writeText?: (s: string) => Promise<void> } }, document?: Document }} [api]
 * @returns {Promise<boolean>}
 */
export async function copyTextToClipboard(value, api = globalThis) {
  const text = String(value ?? "").trim();
  if (!text) return false;

  try {
    const writeText = api?.navigator?.clipboard?.writeText;
    if (typeof writeText === "function") {
      await writeText.call(api.navigator.clipboard, text);
      return true;
    }
  } catch {
    // HTTP, permissão negada ou Clipboard API indisponível — fallback abaixo.
  }

  try {
    const doc = api?.document;
    if (!doc?.body || typeof doc.createElement !== "function") return false;
    const ta = doc.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.left = "-9999px";
    doc.body.appendChild(ta);
    if (typeof ta.select === "function") ta.select();
    const ok = typeof doc.execCommand === "function" ? doc.execCommand("copy") : false;
    ta.remove();
    return Boolean(ok);
  } catch {
    return false;
  }
}
