/**
 * UI Copy Quality Gate
 * Ensures prohibited or technical terms do not leak into customer-facing UI.
 * Scans: .tsx, .ts, .mjs, .js.
 * Excludes: test/spec files, node_modules, .next and comment-only lines.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, extname, basename } from 'node:path';

const APP_DIR = join(import.meta.dirname, '..');
const EXCLUDED_DIRS = new Set(['node_modules', '.next', '__tests__', '__mocks__']);
const SCANNED_EXTENSIONS = new Set(['.tsx', '.ts', '.mjs', '.js']);
const COMMENT_ONLY_LINE = /^\s*(?:\/\/|\/\*|\*|\*\/|\{\s*\/\*)/;

// Small, explicit allowlist for internal technical files that manipulate cache metadata.
const TERM_ALLOWLIST = {
  snapshot: new Set(['lib/reading-copy.mjs', 'lib/reading-state.mjs']),
};

const PLATFORM_VISUAL_PATTERNS = [
  />\s*Platform\s*</g,
  /\b(?:label|title|headline|children|subtitle|text)\s*[:=]\s*["'`]Platform["'`]/g,
  /(?:^|[=:(,\[]\s*)["'`]Platform["'`](?:\s*[),}\]])?/g,
];

const VISIBLE_1970_PATTERNS = [
  /["'`][^"'`]*\b1970\b[^"'`]*["'`]/gi,
  />[^<]*\b1970\b[^<]*</gi,
];

const VISIBLE_TECHNICAL_WORD_PATTERNS = {
  mart: [
    /["'`][^"'`]*\bmart\b[^"'`]*["'`]/gi,
    />[^<]*\bmart\b[^<]*</gi,
  ],
  snapshot: [
    /["'`][^"'`]*\bsnapshot\b[^"'`]*["'`]/gi,
    />[^<]*\bsnapshot\b[^<]*</gi,
  ],
};

const PROHIBITED_RULES = [
  {
    key: 'recorte',
    reason: 'Use "período" ou "período selecionado".',
    patterns: [/\brecorte\b/gi],
  },
  {
    key: 'nao_identificado',
    reason: 'Use um rótulo contextual como "sem cadastro" ou "sem classificação".',
    patterns: [/não identificad[oa]s?/gi],
  },
  {
    key: 'saidas_normais',
    reason: 'Troque por linguagem comercial apropriada, como "Vendas normais".',
    patterns: [/Saídas normais/gi],
  },
  {
    key: 'frescor_operacional',
    reason: 'Não exponha jargão técnico de sincronização ao cliente.',
    patterns: [/Frescor operacional/gi],
  },
  {
    key: 'forma_codigo',
    reason: 'Não exponha códigos técnicos de forma de pagamento.',
    patterns: [/FORMA_/gi],
  },
  {
    key: 'data_sentinela',
    reason: 'Não exponha datas sentinela ao cliente.',
    patterns: [/01\/01\/1970/gi],
  },
  {
    key: 'ano_1970_visivel',
    reason: 'Não exponha datas sentinela ou ano 1970 em texto visível.',
    detect(line) {
      if (line.includes('01/01/1970')) return [];
      return collectMatches(line, VISIBLE_1970_PATTERNS);
    },
  },
  {
    key: 'mart',
    reason: 'Não exponha o termo técnico "mart" em texto visível.',
    detect(line) {
      return collectMatches(line, VISIBLE_TECHNICAL_WORD_PATTERNS.mart);
    },
  },
  {
    key: 'snapshot',
    reason: 'Não exponha o termo técnico "snapshot" em texto visível.',
    detect(line) {
      return collectMatches(line, VISIBLE_TECHNICAL_WORD_PATTERNS.snapshot);
    },
  },
  {
    key: 'trilho_operacional',
    reason: 'Não exponha nomenclatura de trilho interno ao cliente.',
    patterns: [/trilho operacional/gi],
  },
  {
    key: 'publicacao_analitica',
    reason: 'Não exponha nomenclatura de publicação interna ao cliente.',
    patterns: [/publicação analítica/gi],
  },
  {
    key: 'platform_visual',
    reason: 'Use "Plataforma" como label visual; reserve "Platform" para identificadores internos.',
    detect(line) {
      return collectMatches(line, PLATFORM_VISUAL_PATTERNS);
    },
  },
];

function isTestFile(name) {
  return name.includes('.test.') || name.includes('.spec.');
}

function collectSourceFiles(dir, files = []) {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    const stat = statSync(full);
    if (stat.isDirectory()) {
      if (entry.startsWith('.') || EXCLUDED_DIRS.has(entry)) continue;
      collectSourceFiles(full, files);
      continue;
    }
    if (SCANNED_EXTENSIONS.has(extname(full)) && !isTestFile(basename(full))) {
      files.push(full);
    }
  }
  return files;
}

function cloneRegex(pattern) {
  return new RegExp(pattern.source, pattern.flags);
}

function collectMatches(line, patterns) {
  const matches = [];
  for (const pattern of patterns) {
    const regex = cloneRegex(pattern);
    let match = regex.exec(line);
    while (match) {
      matches.push(match[0]);
      if (!regex.global) break;
      match = regex.exec(line);
    }
  }
  return matches;
}

function normalizeSnippet(line) {
  return line.trim().replace(/\s+/g, ' ').slice(0, 180);
}

function isAllowlisted(ruleKey, relativePath) {
  return TERM_ALLOWLIST[ruleKey]?.has(relativePath) || false;
}

function collectViolations(filePath) {
  const relativePath = filePath.replace(APP_DIR + '/', '');
  const lines = readFileSync(filePath, 'utf8').split(/\r?\n/);
  const violations = [];

  for (const [index, line] of lines.entries()) {
    if (!line.trim() || COMMENT_ONLY_LINE.test(line)) continue;

    for (const rule of PROHIBITED_RULES) {
      if (isAllowlisted(rule.key, relativePath)) continue;
      const matches = rule.detect ? rule.detect(line) : collectMatches(line, rule.patterns);
      for (const term of matches) {
        violations.push({
          file: relativePath,
          line: index + 1,
          term: term.trim(),
          reason: rule.reason,
          snippet: normalizeSnippet(line),
        });
      }
    }
  }

  return violations;
}

test('no prohibited UX terms in source files', () => {
  const files = collectSourceFiles(APP_DIR);
  const violations = files.flatMap((file) => collectViolations(file));

  assert.equal(
    violations.length,
    0,
    `Prohibited UI terms found:\n${violations
      .map(({ file, line, term, reason, snippet }) => `- ${file}:${line} [${term}] ${reason} :: ${snippet}`)
      .join('\n')}`
  );
});
