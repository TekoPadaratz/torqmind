/** STT/TTS no browser para o Assistente TorqMind (síntese nativa, sem backend de áudio). */

import {
  browserSpeechSynthesisSupported,
  pickSpeechVoice,
  prepareSpokenText,
} from './spoken-answer.mjs';

export {
  SPEAK_REPLIES_STORAGE_KEY,
  SPOKEN_DETAIL_HINT,
  TTS_BLOCKED_MESSAGE,
  TTS_UNSUPPORTED_MESSAGE,
  browserSpeechSynthesisSupported,
  pickSpeechVoice,
  prepareSpokenText,
  readSpeakRepliesPreference,
  writeSpeakRepliesPreference,
} from './spoken-answer.mjs';

export type BrowserSpeechRecognition = {
  lang: string;
  continuous: boolean;
  interimResults: boolean;
  maxAlternatives: number;
  start: () => void;
  stop: () => void;
  abort: () => void;
  onresult: ((event: BrowserSpeechRecognitionEvent) => void) | null;
  onerror: ((event: { error?: string }) => void) | null;
  onend: (() => void) | null;
};

export type BrowserSpeechRecognitionEvent = {
  resultIndex: number;
  results: {
    length: number;
    [index: number]: {
      isFinal: boolean;
      length: number;
      [index: number]: { transcript: string };
    };
  };
};

type SpeechRecognitionConstructor = new () => BrowserSpeechRecognition;

export function browserSpeechRecognitionConstructor(
  scope: Record<string, unknown> = globalThis as unknown as Record<string, unknown>,
): SpeechRecognitionConstructor | null {
  const candidate = scope.SpeechRecognition ?? scope.webkitSpeechRecognition;
  return typeof candidate === 'function' ? (candidate as SpeechRecognitionConstructor) : null;
}

export function browserSpeechRecognitionSupported(
  scope: Record<string, unknown> = globalThis as unknown as Record<string, unknown>,
): boolean {
  return browserSpeechRecognitionConstructor(scope) !== null;
}

export type SpeakCallbacks = {
  onStart?: () => void;
  onEnd?: () => void;
  onError?: (code: string) => void;
  onBlocked?: () => void;
};

let speakGeneration = 0;
let speakTimer: ReturnType<typeof setTimeout> | null = null;

function clearSpeakTimer() {
  if (speakTimer) {
    clearTimeout(speakTimer);
    speakTimer = null;
  }
}

function preferredVoice(): SpeechSynthesisVoice | null {
  try {
    return pickSpeechVoice(window.speechSynthesis?.getVoices?.() || []);
  } catch {
    return null;
  }
}

/** Pré-carrega a lista de vozes (Chrome entrega vazio até voiceschanged). */
export function warmSpeechVoices(): () => void {
  if (typeof window === 'undefined' || !window.speechSynthesis) return () => undefined;
  const synth = window.speechSynthesis;
  const warm = () => {
    try {
      synth.getVoices();
    } catch {
      /* ignore */
    }
  };
  warm();
  synth.addEventListener?.('voiceschanged', warm);
  return () => {
    try {
      synth.removeEventListener?.('voiceschanged', warm);
    } catch {
      /* ignore */
    }
  };
}

/** Interrompe qualquer leitura em curso. Não tenta contornar bloqueio de reprodução. */
export function stopSpeaking(): void {
  speakGeneration += 1;
  clearSpeakTimer();
  try {
    window.speechSynthesis?.cancel();
  } catch {
    /* ignore */
  }
}

/**
 * Lê o texto preparado com a voz nativa (pt-BR quando houver).
 * Encadeia após cancel() por limitação do Chromium; se o navegador bloquear, avisa via onBlocked.
 */
export function speak(text: string | null | undefined, callbacks?: SpeakCallbacks): void {
  const spoken = prepareSpokenText(text);
  if (typeof window === 'undefined' || !spoken) return;
  if (!browserSpeechSynthesisSupported()) {
    callbacks?.onBlocked?.();
    return;
  }

  const synth = window.speechSynthesis;
  stopSpeaking();
  const myGen = ++speakGeneration;

  // Chromium ignora speak() imediato após cancel(); atraso curto não contorna autoplay.
  speakTimer = setTimeout(() => {
    if (myGen !== speakGeneration) return;
    try {
      const utter = new SpeechSynthesisUtterance(spoken);
      const voice = preferredVoice();
      if (voice) {
        utter.voice = voice;
        utter.lang = voice.lang || 'pt-BR';
      } else {
        utter.lang = 'pt-BR';
      }
      utter.rate = 1;
      utter.onstart = () => {
        if (myGen === speakGeneration) callbacks?.onStart?.();
      };
      utter.onend = () => {
        if (myGen === speakGeneration) callbacks?.onEnd?.();
      };
      utter.onerror = (ev) => {
        if (myGen !== speakGeneration) return;
        const code = String((ev as SpeechSynthesisErrorEvent)?.error || 'error');
        if (code === 'canceled' || code === 'interrupted') {
          callbacks?.onEnd?.();
          return;
        }
        if (code === 'not-allowed') {
          callbacks?.onBlocked?.();
          return;
        }
        callbacks?.onError?.(code);
      };
      synth.speak(utter);
    } catch {
      callbacks?.onBlocked?.();
    }
  }, 80);
}

export type VoiceListenCallbacks = {
  onInterim?: (text: string) => void;
  onFinal?: (text: string) => void;
  onError?: (code: string) => void;
  onEnd?: () => void;
};

/** Inicia reconhecimento contínuo; auto-stop após silêncio (~1,5s) com texto final. */
export function startVoiceListening(
  callbacks: VoiceListenCallbacks,
  options?: { silenceMs?: number },
): () => void {
  const Ctor = browserSpeechRecognitionConstructor();
  if (!Ctor) {
    callbacks.onError?.('unsupported');
    return () => undefined;
  }

  const silenceMs = options?.silenceMs ?? 1500;
  const recognition = new Ctor();
  recognition.lang = 'pt-BR';
  recognition.continuous = true;
  recognition.interimResults = true;
  recognition.maxAlternatives = 1;

  let finalText = '';
  let autoStopTimer: ReturnType<typeof setTimeout> | null = null;
  let stopped = false;

  const clearAutoStop = () => {
    if (autoStopTimer) clearTimeout(autoStopTimer);
    autoStopTimer = null;
  };

  const scheduleAutoStop = () => {
    clearAutoStop();
    if (stopped || !finalText.trim()) return;
    autoStopTimer = setTimeout(() => {
      try {
        recognition.stop();
      } catch {
        /* ignore */
      }
    }, silenceMs);
  };

  const cleanup = () => {
    stopped = true;
    clearAutoStop();
    try {
      recognition.onresult = null;
      recognition.onerror = null;
      recognition.onend = null;
      recognition.abort();
    } catch {
      /* ignore */
    }
  };

  recognition.onresult = (event) => {
    let interim = '';
    let chunkFinal = '';
    for (let i = event.resultIndex; i < event.results.length; i += 1) {
      const part = event.results[i];
      const text = String(part[0]?.transcript || '').trim();
      if (!text) continue;
      if (part.isFinal) chunkFinal += `${text} `;
      else interim += `${text} `;
    }
    if (chunkFinal) {
      finalText = `${finalText} ${chunkFinal}`.trim();
      callbacks.onFinal?.(finalText);
      scheduleAutoStop();
    } else if (interim) {
      callbacks.onInterim?.(`${finalText} ${interim}`.trim());
    }
  };

  recognition.onerror = (ev) => {
    callbacks.onError?.(String(ev?.error || 'error'));
  };

  recognition.onend = () => {
    clearAutoStop();
    if (finalText.trim()) callbacks.onFinal?.(finalText.trim());
    callbacks.onEnd?.();
  };

  try {
    recognition.start();
  } catch {
    callbacks.onError?.('start_failed');
    return cleanup;
  }

  return cleanup;
}
