/** STT/TTS no browser para o Assistente TorqMind (padrão TorqMind-Ops, sem backend de áudio). */

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

export function speak(text: string | null | undefined): void {
  try {
    if (typeof window === 'undefined' || !text) return;
    const synth = window.speechSynthesis;
    if (!synth) return;
    const utter = new SpeechSynthesisUtterance(text);
    utter.lang = 'pt-BR';
    synth.cancel();
    synth.speak(utter);
  } catch {
    /* TTS best-effort */
  }
}

export function stopSpeaking(): void {
  try {
    window.speechSynthesis?.cancel();
  } catch {
    /* ignore */
  }
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
