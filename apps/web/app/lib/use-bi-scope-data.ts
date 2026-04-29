'use client';

import { useEffect, useRef, useState } from 'react';
import { useRouter } from 'next/navigation';

import { extractApiError } from './errors';
import { isRequestCanceled } from './api';
import { finishScopeTransition, loadStableScopeData } from './scope-runtime';
import type { ScopeQuery } from './scope';
import { loadSession, readCachedSession } from './session';

type UseBiScopeDataOptions<T> = {
  moduleKey: string;
  scope: ScopeQuery;
  errorMessage: string;
  buildRequestUrl: (scope: ScopeQuery, session: any) => string | null;
  requestTimeoutMs?: number;
  unavailableRetryAttempts?: number;
  unavailableRetryDelayMs?: number;
};

export function useBiScopeData<T>({
  moduleKey,
  scope,
  errorMessage,
  buildRequestUrl,
  requestTimeoutMs,
  unavailableRetryAttempts = 4,
  unavailableRetryDelayMs = 2_000,
}: UseBiScopeDataOptions<T>) {
  const router = useRouter();
  const activeRequestRef = useRef('');

  const [claims, setClaims] = useState<any>(readCachedSession());
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [pendingUnavailable, setPendingUnavailable] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    const requestToken = `${moduleKey}:${scope.scope_key}:${scope.scope_epoch}`;
    activeRequestRef.current = requestToken;

    const controller = new AbortController();
    let disposed = false;

    const waitBeforeRetry = (ms: number) =>
      new Promise<void>((resolve, reject) => {
        if (controller.signal.aborted) {
          reject(new DOMException('The operation was aborted.', 'AbortError'));
          return;
        }
        const timer = window.setTimeout(resolve, ms);
        controller.signal.addEventListener(
          'abort',
          () => {
            window.clearTimeout(timer);
            reject(new DOMException('The operation was aborted.', 'AbortError'));
          },
          { once: true },
        );
      });

    const load = async () => {
      setLoading(true);
      setPendingUnavailable(false);
      setError('');
      setData(null);

      try {
        const me = await loadSession(router, 'product');
        if (!me) return;
        if (disposed || activeRequestRef.current !== requestToken) return;
        setClaims(me);

        if (!scope.dt_ini || !scope.dt_fim) {
          router.replace(me?.home_path || '/dashboard');
          return;
        }

        const requestUrl = buildRequestUrl(scope, me);
        if (!requestUrl) {
          finishScopeTransition(scope, moduleKey, false);
          return;
        }

        let payload: T | null = null;
        for (let attempt = 0; attempt <= unavailableRetryAttempts; attempt += 1) {
          payload = await loadStableScopeData<T>({
            moduleKey,
            requestUrl,
            scope,
            signal: controller.signal,
            requestTimeoutMs,
          });
          if (payload || attempt >= unavailableRetryAttempts) break;
          await waitBeforeRetry(unavailableRetryDelayMs);
        }

        if (disposed || activeRequestRef.current !== requestToken) return;

        if (!payload) {
          setPendingUnavailable(true);
          finishScopeTransition(scope, moduleKey, false);
          return;
        }

        setData(payload);
        finishScopeTransition(scope, moduleKey, true);
      } catch (err: any) {
        if (disposed || activeRequestRef.current !== requestToken || isRequestCanceled(err)) return;
        setError(extractApiError(err, errorMessage));
        finishScopeTransition(scope, moduleKey, false);
      } finally {
        if (!disposed && activeRequestRef.current === requestToken) {
          setLoading(false);
        }
      }
    };

    void load();

    return () => {
      disposed = true;
      controller.abort();
    };
  }, [errorMessage, moduleKey, requestTimeoutMs, router, scope, unavailableRetryAttempts, unavailableRetryDelayMs]);

  return {
    claims,
    data,
    error,
    loading,
    pendingUnavailable,
  };
}
