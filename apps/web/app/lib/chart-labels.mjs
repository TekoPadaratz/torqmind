const NAME_PARTICLES = new Set(["de", "da", "do", "dos", "das", "e", "di", "del"]);
const MISSING_PERSON = "Nome não cadastrado";

function collapseSpaces(value) {
  return String(value ?? "")
    .replace(/\s+/g, " ")
    .trim();
}

export function honestPersonName(raw, fallback) {
  const text = collapseSpaces(raw);
  if (!text) return fallback || MISSING_PERSON;
  if (/^funcion[aá]rio\s*#\s*\d+$/i.test(text)) return text;
  if (/^\d+$/.test(text)) return fallback || MISSING_PERSON;
  return text;
}

function tokenizePersonName(name) {
  const text = collapseSpaces(name);
  if (/^funcion[aá]rio\s*#\s*\d+$/i.test(text)) return [text];
  return text.split(" ").filter(Boolean);
}

function isNameParticle(token) {
  return NAME_PARTICLES.has(String(token || "").toLocaleLowerCase("pt-BR"));
}

function nextNameUnit(tokens, startIndex) {
  let i = startIndex;
  const prefix = [];
  while (i < tokens.length && isNameParticle(tokens[i])) {
    prefix.push(tokens[i]);
    i += 1;
  }
  if (i >= tokens.length) {
    return { unit: prefix.join(" "), nextIndex: i };
  }
  prefix.push(tokens[i]);
  return { unit: prefix.join(" "), nextIndex: i + 1 };
}

function buildPersonShort(tokens, unitsWanted) {
  if (!tokens.length) return MISSING_PERSON;
  const parts = [tokens[0]];
  let index = 1;
  let added = 1;
  while (added < unitsWanted && index < tokens.length) {
    const step = nextNameUnit(tokens, index);
    if (!step.unit) break;
    parts.push(step.unit);
    index = step.nextIndex;
    added += 1;
  }
  return parts.join(" ");
}

function keyOf(id) {
  return String(id);
}

/**
 * Rótulos curtos para pessoas no conjunto exibido.
 * A chave do mapa é o id informado — nunca o nome abreviado.
 */
export function shortPersonLabels(items) {
  const rows = (Array.isArray(items) ? items : []).map((item, index) => {
    const id = item?.id;
    const fallback =
      id != null && String(id).trim() !== "" ? `Funcionário #${id}` : MISSING_PERSON;
    const full = honestPersonName(item?.name, fallback);
    return {
      key: id == null ? `missing:${index}` : keyOf(id),
      full,
      tokens: tokenizePersonName(full),
    };
  });

  const labels = new Map();
  const unitsWanted = new Map();
  for (const row of rows) {
    unitsWanted.set(row.key, 1);
    labels.set(row.key, buildPersonShort(row.tokens, 1));
  }

  const collidingKeys = () => {
    const groups = new Map();
    for (const row of rows) {
      const bucket = String(labels.get(row.key) || "").toLocaleLowerCase("pt-BR");
      const list = groups.get(bucket) || [];
      list.push(row);
      groups.set(bucket, list);
    }
    const keys = new Set();
    for (const list of groups.values()) {
      const fulls = new Set(list.map((row) => row.full));
      if (fulls.size > 1) {
        for (const row of list) keys.add(row.key);
      }
    }
    return keys;
  };

  for (let step = 0; step < 8; step += 1) {
    const keys = collidingKeys();
    if (keys.size === 0) break;
    let progressed = false;
    for (const row of rows) {
      if (!keys.has(row.key)) continue;
      const next = (unitsWanted.get(row.key) || 1) + 1;
      if (next > row.tokens.length) continue;
      unitsWanted.set(row.key, next);
      labels.set(row.key, buildPersonShort(row.tokens, next));
      progressed = true;
    }
    if (!progressed) break;
  }

  return labels;
}

export function ellipsizeLabel(text, maxChars = 18) {
  const value = collapseSpaces(text);
  if (!value) return "—";
  const limit = Number(maxChars);
  if (!Number.isFinite(limit) || limit < 2) return value;
  if (value.length <= limit) return value;
  return `${value.slice(0, limit - 1)}…`;
}

/**
 * Abreviação contextual para produtos/filiais/categorias.
 * Expande o corte quando a reticência colidir no conjunto.
 */
export function shortCategoryLabels(items, maxChars = 22) {
  const rows = (Array.isArray(items) ? items : []).map((item, index) => {
    const id = item?.id;
    const full = collapseSpaces(item?.name) || "—";
    return {
      key: id == null ? `missing:${index}` : keyOf(id),
      full,
    };
  });

  let limit = Math.max(4, Number(maxChars) || 22);
  const labels = new Map();

  while (limit <= 80) {
    labels.clear();
    const groups = new Map();
    let colliding = false;
    for (const row of rows) {
      const short = ellipsizeLabel(row.full, limit);
      labels.set(row.key, short);
      const bucket = short.toLocaleLowerCase("pt-BR");
      const list = groups.get(bucket) || [];
      list.push(row.key);
      groups.set(bucket, list);
      if (list.length > 1) {
        const sameFull = list.every((key) => {
          const other = rows.find((r) => r.key === key);
          return other && other.full === row.full;
        });
        if (!sameFull) colliding = true;
      }
    }
    if (!colliding) break;
    const longest = rows.reduce((acc, row) => Math.max(acc, row.full.length), 0);
    if (limit >= longest) break;
    limit += 4;
  }

  return labels;
}
