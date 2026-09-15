const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const ts = require('typescript');

// Exercise the actual component's event handlers and effects without a backend.
function mount({ saved = null, post, history = Promise.resolve({ items: [] }) } = {}) {
  const slots = [], effects = [], storage = new Map();
  if (saved) storage.set('torqmind.ai.conversation.1', saved);
  let cursor = 0, tree, company = '1';
  const hooks = {
    useState(initial) {
      const i = cursor++;
      if (!(i in slots)) slots[i] = initial;
      return [slots[i], value => { slots[i] = typeof value === 'function' ? value(slots[i]) : value; }];
    },
    useRef(initial) { const i = cursor++; return slots[i] ||= { current: initial }; },
    useId() { cursor++; return 'assistant'; },
    useMemo(fn, deps) {
      const i = cursor++;
      if (!slots[i] || deps.some((v, j) => !Object.is(v, slots[i].deps[j]))) slots[i] = { deps, value: fn() };
      return slots[i].value;
    },
    useCallback(fn, deps) { return hooks.useMemo(() => fn, deps); },
    useEffect(fn, deps) {
      const i = cursor++, old = slots[i];
      if (!old || deps.some((v, j) => !Object.is(v, old.deps[j]))) {
        slots[i] = { deps, cleanup: old?.cleanup };
        effects.push(() => { slots[i].cleanup?.(); slots[i].cleanup = fn(); });
      }
    },
  };
  const voice = new Proxy({}, { get: () => () => false });
  const api = {
    apiPost: post || (async () => ({ id: 'new', answer_text: 'Resposta' })),
    apiGet: async path => path.includes('/messages') ? history : { items: [] },
  };
  const compiled = ts.transpileModule(fs.readFileSync('app/components/intelligence/IntelligenceHost.tsx', 'utf8'), {
    compilerOptions: { jsx: ts.JsxEmit.ReactJSX, module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  }).outputText;
  const module = { exports: {} };
  vm.runInNewContext(compiled, {
    module, exports: module.exports, queueMicrotask,
    window: { addEventListener() {}, removeEventListener() {} },
    sessionStorage: { getItem: k => storage.get(k) || null, setItem: (k,v) => storage.set(k,v), removeItem: k => storage.delete(k) },
    require(name) {
      if (name === 'react') return { ...hooks, Fragment: require('react').Fragment };
      if (name === 'react/jsx-runtime') return require(name);
      if (name === 'next/navigation') return { usePathname: () => '/sales' };
      if (name === 'next/image' || name === 'next/link') return { default: () => null };
      if (name.endsWith('/api')) return api;
      if (name.endsWith('/auth')) return { getClaims: () => ({ id_empresa: 1 }), hasSession: () => true, requireAuth: () => true };
      if (name.endsWith('/scope')) return { useScopeQuery: () => ({ id_empresa: company, id_filiais: [], dt_ini: '2026-09-01', dt_fim: '2026-09-14' }) };
      if (name.endsWith('/session')) return { readCachedSession: () => ({}), canAccessScreenKey: () => true };
      if (name.endsWith('/voice-assistant')) return voice;
      throw Error(name);
    },
  });
  function render() { cursor = 0; tree = module.exports.default(); effects.splice(0).forEach(fn => fn()); }
  function nodes(value) {
    if (!value) return [];
    if (Array.isArray(value)) return value.flatMap(v => nodes(v));
    return typeof value === 'object' ? [value, ...nodes(value.props?.children)] : [];
  }
  const button = label => nodes(tree).find(n => n.type === 'button' && (n.props.children === label || n.props['aria-label'] === label));
  render(); render(); button('Abrir Assistente TorqMind').props.onClick(); render(); render();
  return { render, button, storage, buttonCount: label => nodes(tree).filter(n => n.type === 'button' && n.props.children === label).length, setCompany: value => { company = value; render(); }, text: () => JSON.stringify(tree), async flush() { for (let i = 0; i < 8; i++) { await Promise.resolve(); render(); } } };
}

test('rapid suggestions create one conversation and one question', async () => {
  const calls = [];
  let release;
  const pending = new Promise(resolve => { release = resolve; });
  const ui = mount({ post: (path) => { calls.push(path); return path === '/ai/conversations' ? pending : Promise.resolve({ answer_text: 'Pronto' }); } });
  const click = ui.button('Investigar vendas').props.onClick;
  click(); click();
  assert.equal(calls.length, 1);
  release({ id: 'one' }); await ui.flush();
  assert.deepEqual(calls, ['/ai/conversations', '/ai/conversations/one/messages']);
});

test('new conversation drops the local pointer and creates a fresh conversation only on send', async () => {
  const calls = [];
  const ui = mount({ saved: 'old', post: async path => { calls.push(path); return { id: 'fresh', answer_text: 'Pronto' }; } });
  await ui.flush();
  ui.button('Nova conversa').props.onClick(); await ui.flush();
  assert.equal(ui.storage.size, 0);
  assert.equal(calls.length, 0);
  ui.button('Investigar carteira').props.onClick(); await ui.flush();
  assert.deepEqual(calls, ['/ai/conversations', '/ai/conversations/fresh/messages']);
});

test('failed send unlocks the next attempt', async () => {
  let attempts = 0;
  const ui = mount({ saved: 'old', post: async () => { attempts++; throw Error('offline'); } });
  await ui.flush();
  ui.button('Investigar vendas').props.onClick(); await ui.flush();
  ui.button('Investigar vendas').props.onClick(); await ui.flush();
  assert.equal(attempts, 2);
});

test('late history cannot overwrite an answer to a newly sent question', async () => {
  let release;
  const history = new Promise(resolve => { release = resolve; });
  const ui = mount({ saved: 'old', history, post: async () => ({ answer_text: 'Resposta atual' }) });
  await ui.flush();
  ui.button('Investigar vendas').props.onClick(); await ui.flush();
  release({ items: [{ role: 'assistant', content_text: 'Resposta antiga' }] });
  await ui.flush();
  assert.ok(ui.text().includes('Resposta atual'));
  assert.ok(!ui.text().includes('Resposta antiga'));
});

test('switching company never stores the previous company conversation under the new company', async () => {
  const ui = mount({ saved: 'company-one' });
  await ui.flush();
  ui.setCompany('8'); await ui.flush();
  assert.equal(ui.storage.get('torqmind.ai.conversation.1'), 'company-one');
  assert.equal(ui.storage.has('torqmind.ai.conversation.8'), false);
});

test('clarification choices are not repeated as suggestions', async () => {
  const ui = mount({ saved: 'old', post: async () => ({
    answer_text: 'Qual carteira?', clarification_options: [{ label: 'Recebimentos', value: 'receber' }],
    suggestions: ['Recebimentos', 'Ver filiais'],
  }) });
  await ui.flush();
  ui.button('Investigar carteira').props.onClick(); await ui.flush();
  assert.equal(ui.buttonCount('Recebimentos'), 1);
  assert.equal(ui.buttonCount('Ver filiais'), 1);
});
