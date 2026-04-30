/**
 * UI Copy Quality Gate
 * Ensures prohibited/low-quality terms do not appear in user-facing pages.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, extname } from 'node:path';

const PAGES_DIR = join(import.meta.dirname, '..');

const PROHIBITED_TERMS = [
  { pattern: /\brecorte\b/gi, label: 'recorte (use "período")' },
  { pattern: /não identificad[oa]/gi, label: 'não identificado/a (use "sem cadastro" ou contexto)' },
];

// Collect all .tsx page files recursively
function collectPageFiles(dir, files = []) {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    const stat = statSync(full);
    if (stat.isDirectory() && !entry.startsWith('.') && entry !== 'node_modules') {
      collectPageFiles(full, files);
    } else if (extname(full) === '.tsx' || extname(full) === '.ts') {
      files.push(full);
    }
  }
  return files;
}

test('no prohibited UX terms in page files', () => {
  const files = collectPageFiles(PAGES_DIR);
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
