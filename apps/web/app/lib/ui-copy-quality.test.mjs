/**
 * UI Copy Quality Gate
 * Ensures prohibited/low-quality terms do not appear in user-facing pages.
 * Scans: .tsx, .ts, .mjs, .js (excludes test files, node_modules, .next)
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, extname, basename } from 'node:path';

const PAGES_DIR = join(import.meta.dirname, '..');

const PROHIBITED_TERMS = [
  { pattern: /\brecorte\b/gi, label: 'recorte (use "período")' },
  { pattern: /não identificad[oa]s?/gi, label: 'não identificado/a (use "sem cadastro" ou contexto)' },
];

// Directories and filename patterns to exclude from scanning
const EXCLUDED_DIRS = new Set(['node_modules', '.next', '__tests__', '__mocks__']);
const isTestFile = (name) => name.includes('.test.') || name.includes('.spec.');
const SCANNED_EXTENSIONS = new Set(['.tsx', '.ts', '.mjs', '.js']);

// Collect all UI source files recursively (excludes tests and build output)
function collectSourceFiles(dir, files = []) {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    const stat = statSync(full);
    if (stat.isDirectory()) {
      if (entry.startsWith('.') || EXCLUDED_DIRS.has(entry)) continue;
      collectSourceFiles(full, files);
    } else if (SCANNED_EXTENSIONS.has(extname(full)) && !isTestFile(basename(full))) {
      files.push(full);
    }
  }
  return files;
}

test('no prohibited UX terms in source files', () => {
  const files = collectSourceFiles(PAGES_DIR);
  const violations = [];

  for (const file of files) {
    const content = readFileSync(file, 'utf8');
    for (const { pattern, label } of PROHIBITED_TERMS) {
      pattern.lastIndex = 0;
      const matches = content.match(pattern);
      if (matches) {
        const rel = file.replace(PAGES_DIR + '/', '');
        violations.push(`${rel}: found "${matches[0]}" → ${label}`);
      }
    }
  }

  assert.equal(
    violations.length,
    0,
    `Prohibited UI terms found:\n${violations.join('\n')}`
  );
});
