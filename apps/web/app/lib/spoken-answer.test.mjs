import assert from 'node:assert/strict';
import test from 'node:test';

import {
  SPEAK_REPLIES_STORAGE_KEY,
  SPOKEN_DETAIL_HINT,
  TTS_UNSUPPORTED_MESSAGE,
  browserSpeechSynthesisSupported,
  pickSpeechVoice,
  prepareSpokenText,
  readSpeakRepliesPreference,
  writeSpeakRepliesPreference,
} from './spoken-answer.mjs';

test('prepareSpokenText devolve vazio para nulo ou branco', () => {
  assert.equal(prepareSpokenText(''), '');
  assert.equal(prepareSpokenText(null), '');
  assert.equal(prepareSpokenText('   '), '');
});

test('prepareSpokenText preserva reais, datas e avisos curtos', () => {
  const raw = 'Carteira a receber: R$ 8.313.997,45. Posição em 13/09/2026. A vencer.';
  assert.equal(prepareSpokenText(raw), raw);
});

test('prepareSpokenText remove Markdown, URL, UUID e códigos técnicos', () => {
  const raw = [
    '**Carteira** em `/bi/finance/overview`',
    'Veja https://www.torqmind.com.br/finance e www.exemplo.com',
    'Código `openai_not_configured` id_empresa 1 conversa 11111111-2222-3333-4444-555555555555',
  ].join('\n');
  const spoken = prepareSpokenText(raw);
  assert.match(spoken, /Carteira/);
  assert.ok(!spoken.includes('**'));
  assert.ok(!spoken.includes('https://'));
  assert.ok(!spoken.includes('www.exemplo'));
  assert.ok(!spoken.includes('/bi/finance'));
  assert.ok(!spoken.includes('openai_not_configured'));
  assert.ok(!spoken.includes('id_empresa'));
  assert.ok(!spoken.includes('11111111-2222-3333-4444-555555555555'));
});

test('prepareSpokenText resume resposta longa e aponta o detalhe na tela', () => {
  const raw = [
    'Faturamento caiu R$ 813.717,20 (-8,6%): R$ 8.692.918,23 em 01/09/2026 → 13/09/2026 vs R$ 9.506.635,43 no período anterior.',
    'Filial, grupo e hora mostram a mesma variação por ângulos diferentes. Não some os três.',
    'O período atual ainda não terminou. Compare com cautela.',
    'Principais filiais:',
    '- VR 05: queda de R$ 521.809,71',
    '- VR 01: queda de R$ 323.901,50',
    '- VR 07: queda de R$ 64.754,81',
    'Os números acima mostram contribuições, não uma causa comprovada.',
  ].join('\n\n');
  const spoken = prepareSpokenText(raw);
  assert.match(spoken, /R\$ 813\.717,20/);
  assert.match(spoken, /cautela/i);
  assert.match(spoken, new RegExp(SPOKEN_DETAIL_HINT));
  assert.ok(!spoken.includes('VR 07: queda'));
  assert.ok(spoken.length < raw.length);
});

test('preferência Responder por voz começa desligada e persiste no storage existente', () => {
  const mem = new Map();
  const storage = {
    getItem: (key) => (mem.has(key) ? mem.get(key) : null),
    setItem: (key, value) => {
      mem.set(key, String(value));
    },
  };
  assert.equal(readSpeakRepliesPreference(storage), false);
  writeSpeakRepliesPreference(true, storage);
  assert.equal(mem.get(SPEAK_REPLIES_STORAGE_KEY), '1');
  assert.equal(readSpeakRepliesPreference(storage), true);
  writeSpeakRepliesPreference(false, storage);
  assert.equal(mem.get(SPEAK_REPLIES_STORAGE_KEY), '0');
  assert.equal(readSpeakRepliesPreference(storage), false);
});

test('browserSpeechSynthesisSupported distingue síntese de reconhecimento', () => {
  assert.equal(browserSpeechSynthesisSupported({}), false);
  assert.equal(
    browserSpeechSynthesisSupported({
      SpeechRecognition: function SpeechRecognition() {},
    }),
    false,
  );
  assert.equal(
    browserSpeechSynthesisSupported({
      speechSynthesis: { speak() {} },
      SpeechSynthesisUtterance: function SpeechSynthesisUtterance() {},
    }),
    true,
  );
});

test('pickSpeechVoice prefere pt-BR e aceita alternativa', () => {
  const voices = [
    { lang: 'en-US', name: 'English' },
    { lang: 'pt-PT', name: 'Português PT' },
    { lang: 'pt-BR', name: 'Português BR' },
  ];
  assert.equal(pickSpeechVoice(voices).name, 'Português BR');
  assert.equal(pickSpeechVoice(voices.filter((v) => v.lang !== 'pt-BR')).name, 'Português PT');
  assert.equal(pickSpeechVoice(voices.filter((v) => v.lang === 'en-US')).name, 'English');
  assert.equal(pickSpeechVoice([]), null);
});

test('mensagem de indisponibilidade é clara e não técnica', () => {
  assert.match(TTS_UNSUPPORTED_MESSAGE, /não lê respostas em voz/i);
  assert.ok(!TTS_UNSUPPORTED_MESSAGE.includes('speechSynthesis'));
});
