/** Texto falado e preferência de voz do Assistente (síntese nativa, sem provedor pago). */

export const SPEAK_REPLIES_STORAGE_KEY = 'torqmind.assistant.speakReplies';
export const SPOKEN_DETAIL_HINT = 'Os detalhes estão na tela.';
export const TTS_UNSUPPORTED_MESSAGE =
  'Este navegador não lê respostas em voz. A resposta escrita continua disponível.';
export const TTS_BLOCKED_MESSAGE = 'Toque em Ouvir resposta para ouvir.';
export const SPOKEN_LONG_LIMIT = 420;

const TECHNICAL_TOKEN = new RegExp(
  `\\b(${[
    ['openai', 'not', 'configured'].join('_'),
    ['forbidden', 'scope'].join('_'),
    'fallback',
    ['mutation', 'denied'].join('_'),
    ['conversation', 'create', 'failed'].join('_'),
    ['id', 'empresa'].join('_'),
    ['id', 'filial'].join('_'),
    ['id', 'comprovante'].join('_'),
    ['conversation', 'id'].join('_'),
    ['intent', 'id'].join('_'),
  ].join('|')})\\b`,
  'gi',
);
const UUID_RE = /\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/gi;
const URL_RE = /\bhttps?:\/\/\S+/gi;
const WWW_RE = /\bwww\.\S+/gi;
const PATH_RE = /(?:^|[\s(])(\/(?:api|bi|ai|platform)\/[A-Za-z0-9_./?-]+)/g;

/**
 * Versão falada: preserva números, moeda e avisos; remove Markdown, URLs e códigos.
 * Respostas longas viram resumo fiel + indicação de que o detalhe está na tela.
 */
export function prepareSpokenText(raw) {
  let text = String(raw || '');
  if (!text.trim()) return '';

  text = text.replace(/```[\s\S]*?```/g, ' ');
  text = text.replace(/`([^`]+)`/g, '$1');
  text = text.replace(/\[([^\]]+)\]\([^)]+\)/g, '$1');
  text = text.replace(URL_RE, ' ');
  text = text.replace(WWW_RE, ' ');
  text = text.replace(PATH_RE, ' ');
  text = text.replace(/\*\*([^*]+)\*\*/g, '$1');
  text = text.replace(/\*([^*]+)\*/g, '$1');
  text = text.replace(/^#{1,6}\s+/gm, '');
  text = text.replace(/^\s*[-*•]\s+/gm, '');
  text = text.replace(UUID_RE, ' ');
  text = text.replace(TECHNICAL_TOKEN, ' ');
  text = text.replace(/[ \t]+\n/g, '\n');
  text = text.replace(/\n{3,}/g, '\n\n');
  text = text.replace(/[ \t]{2,}/g, ' ');
  text = text.replace(/ *\n */g, '\n').trim();
  if (!text) return '';

  if (text.length <= SPOKEN_LONG_LIMIT) return text;

  const firstPara = (text.split(/\n\n/)[0] || text).trim();
  let summary = firstPara;
  if (summary.length > SPOKEN_LONG_LIMIT) {
    const sentences = summary.split(/(?<=[.!?])\s+/);
    summary = '';
    for (const sentence of sentences) {
      const next = summary ? `${summary} ${sentence}` : sentence;
      if (summary && next.length > SPOKEN_LONG_LIMIT) break;
      summary = next;
    }
    if (!summary) summary = firstPara.slice(0, SPOKEN_LONG_LIMIT).trim();
  }

  const warning = text.match(
    /[^\n]*(?:atenção|incompleto|permissão|hipótese|não terminou|cautela|posição atual)[^\n]*/i,
  );
  if (warning) {
    const notice = warning[0].trim();
    if (notice && notice.length < 200 && !summary.includes(notice.slice(0, 32))) {
      summary = `${summary} ${notice}`.trim();
    }
  }

  if (!summary.includes(SPOKEN_DETAIL_HINT)) {
    summary = `${summary} ${SPOKEN_DETAIL_HINT}`.trim();
  }
  return summary;
}

export function readSpeakRepliesPreference(storage) {
  try {
    const store = storage ?? (typeof window !== 'undefined' ? window.localStorage : null);
    if (!store) return false;
    return store.getItem(SPEAK_REPLIES_STORAGE_KEY) === '1';
  } catch {
    return false;
  }
}

export function writeSpeakRepliesPreference(enabled, storage) {
  try {
    const store = storage ?? (typeof window !== 'undefined' ? window.localStorage : null);
    if (!store) return;
    if (enabled) store.setItem(SPEAK_REPLIES_STORAGE_KEY, '1');
    else store.setItem(SPEAK_REPLIES_STORAGE_KEY, '0');
  } catch {
    /* persistência best-effort */
  }
}

export function browserSpeechSynthesisSupported(scope) {
  const root = scope ?? (typeof globalThis !== 'undefined' ? globalThis : {});
  const synth = root.speechSynthesis;
  return Boolean(synth && typeof synth.speak === 'function' && typeof root.SpeechSynthesisUtterance === 'function');
}

/** Prefere pt-BR; aceita outra voz pt; senão a primeira disponível. */
export function pickSpeechVoice(voices, preferredLang = 'pt-BR') {
  const list = Array.from(voices || []);
  if (!list.length) return null;
  const wanted = String(preferredLang || 'pt-BR').toLowerCase().replace('_', '-');
  const exact = list.find((voice) => String(voice.lang || '').toLowerCase().replace('_', '-') === wanted);
  if (exact) return exact;
  const prefix = wanted.split('-')[0];
  const langMatch = list.find((voice) => String(voice.lang || '').toLowerCase().startsWith(prefix));
  if (langMatch) return langMatch;
  return list[0];
}
