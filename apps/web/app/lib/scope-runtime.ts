'use client';

import { useSyncExternalStore } from 'react';

import { apiGet } from './api';
import { resolvePricingOverviewRequest } from './pricing-request.mjs';
import { isScopePayloadStable } from './reading-state.mjs';
import { buildProductHref, buildScopeKey, PRODUCT_LINKS } from './product-scope.mjs';
import type { ScopeQuery } from './scope';
import { buildScopeParams } from './scope';

type ScopeLike = Partial<ScopeQuery>;

type ScopeTransitionState = {
  active: boolean;
  moduleKey: string | null;
  ready: boolean | null;
  scope: ScopeLike | null;
  startedAt: number | null;
};

type PrefetchDefinition = {
  moduleKey: string;
  buildUrl: (scope: ScopeLike) => string | null;
  requestTimeoutMs?: number;
};

const STABLE_POLL_MS = 1200;
const STABLE_MAX_WAIT_MS = 9000;

const stablePayloadCache = new Map<string, any>();
const pendingPayloadRequests = new Map<string, Promise<any | null>>();

let activePrefetchController: AbortController | null = null;
let scopeTransitionState: ScopeTransitionState = {
  active: false,
  moduleKey: null,
  ready: null,
  scope: null,
  startedAt: null,
};

const transitionListeners = new Set<() => void>();

function emitScopeTransition() {
  transitionListeners.forEach((listener) => listener());
}

function normalizeScope(scope: ScopeLike): ScopeLike {
  const id_filiais = Array.isArray(scope?.id_filiais) ? scope.id_filiais : [];
  return {
    ...scope,
    scope_key: scope?.scope_key || buildScopeKey(scope || {}),
    scope_epoch: scope?.scope_epoch || `legacy:${buildScopeKey(scope || {})}`,
    id_filial: scope?.id_filial || (id_filiais.length === 1 ? id_filiais[0] : null),
    id_filiais,
  };
}

function buildScopedRequestCacheKey(moduleKey: string, requestUrl: string, scope: ScopeLike) {
  const normalized = normalizeScope(scope);
  return `${moduleKey}::${normalized.scope_key}::${requestUrl}`;
}

function buildScopeRequestHeaders(scope: ScopeLike) {
  const normalized = normalizeScope(scope);
  return {
    'X-Torq-Scope-Epoch': String(normalized.scope_epoch || ''),
    'X-Torq-Scope-Key': String(normalized.scope_key || ''),
  };
}

function sleepWithAbort(ms: number, signal?: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException('The operation was aborted.', 'AbortError'));
      return;
    }

    const timer = window.setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);

    const onAbort = () => {
      window.clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
      reject(new DOMException('The operation was aborted.', 'AbortError'));
    };

    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

export function startScopeTransition(scope: ScopeLike, moduleKey = 'product_scope') {
  scopeTransitionState = {
    active: true,
    moduleKey,
    ready: false,
    scope: normalizeScope(scope),
    startedAt: Date.now(),
  };
  emitScopeTransition();
}

export function finishScopeTransition(scope: ScopeLike, moduleKey: string, ready: boolean) {
  const normalized = normalizeScope(scope);
  const currentScopeEpoch = String(scopeTransitionState.scope?.scope_epoch || '');
  if (!scopeTransitionState.active || currentScopeEpoch !== String(normalized.scope_epoch || '')) {
    return;
  }

  scopeTransitionState = {
    active: false,
    moduleKey,
    ready,
    scope: normalized,
    startedAt: scopeTransitionState.startedAt,
  };
  emitScopeTransition();
}

export function useScopeTransitionState() {
  return useSyncExternalStore(
    (listener) => {
      transitionListeners.add(listener);
      return () => transitionListeners.delete(listener);
    },
    () => scopeTransitionState,
    () => scopeTransitionState,
  );
}

export function readStableScopePayload(moduleKey: string, requestUrl: string, scope: ScopeLike) {
  return stablePayloadCache.get(buildScopedRequestCacheKey(moduleKey, requestUrl, scope)) || null;
}

export async function loadStableScopeData<T>({
  moduleKey,
  requestUrl,
  scope,
  signal,
  requestTimeoutMs,
  maxWaitMs = STABLE_MAX_WAIT_MS,
  pollMs = STABLE_POLL_MS,
}: {
  moduleKey: string;
  requestUrl: string;
  scope: ScopeLike;
  signal?: AbortSignal;
  requestTimeoutMs?: number;
  maxWaitMs?: number;
  pollMs?: number;
}): Promise<T | null> {
  const cacheKey = buildScopedRequestCacheKey(moduleKey, requestUrl, scope);
  const cached = stablePayloadCache.get(cacheKey);
  if (cached) return cached as T;

  if (pendingPayloadRequests.has(cacheKey)) {
    return (await pendingPayloadRequests.get(cacheKey)) as T | null;
  }

  const promise = (async () => {
    const deadline = Date.now() + Math.max(maxWaitMs, pollMs);

    while (true) {
      if (signal?.aborted) {
        throw new DOMException('The operation was aborted.', 'AbortError');
      }

      const payload = await apiGet(requestUrl, {
        signal,
        headers: buildScopeRequestHeaders(scope),
        timeout: requestTimeoutMs,
      });

      if (isScopePayloadStable(payload)) {
        stablePayloadCache.set(cacheKey, payload);
        return payload as T;
      }

      if (Date.now() >= deadline) return null;
      await sleepWithAbort(pollMs, signal);
    }
  })()
    .finally(() => {
      pendingPayloadRequests.delete(cacheKey);
    });

  pendingPayloadRequests.set(cacheKey, promise);
  return (await promise) as T | null;
}

const PREFETCH_DEFINITIONS: PrefetchDefinition[] = [
  {
    moduleKey: 'dashboard_home',
    buildUrl: (scope) => `/bi/dashboard/home?${buildScopeParams(scope).toString()}`,
    requestTimeoutMs: 60_000,
  },
  {
    moduleKey: 'sales_overview',
    buildUrl: (scope) => `/bi/sales/overview?${buildScopeParams(scope).toString()}`,
  },
  {
    moduleKey: 'cash_overview',
    buildUrl: (scope) => `/bi/cash/overview?${buildScopeParams(scope).toString()}`,
  },
  {
    moduleKey: 'fraud_overview',
    buildUrl: (scope) => `/bi/fraud/overview?${buildScopeParams(scope).toString()}`,
  },
  {
    moduleKey: 'customers_overview',
    buildUrl: (scope) => `/bi/customers/overview?${buildScopeParams(scope).toString()}`,
  },
  {
    moduleKey: 'finance_overview',
    buildUrl: (scope) => `/bi/finance/overview?${buildScopeParams(scope).toString()}&include_operational=false`,
  },
  {
    moduleKey: 'goals_overview',
    buildUrl: (scope) => `/bi/goals/overview?${buildScopeParams(scope).toString()}`,
  },
  {
    moduleKey: 'pricing_competitor_overview',
    buildUrl: (scope) => resolvePricingOverviewRequest(scope).requestUrl,
  },
];

async function runPrefetchQueue(tasks: Array<() => Promise<void>>, concurrency = 3) {
  const queue = [...tasks];
  const workers = Array.from({ length: Math.min(concurrency, queue.length) }, async () => {
    while (queue.length) {
      const task = queue.shift();
      if (!task) return;
      await task();
    }
  });
  await Promise.allSettled(workers);
}

export function prefetchProductScope(scope: ScopeLike, router?: { prefetch?: (href: string) => void }) {
  const normalized = normalizeScope(scope);

  activePrefetchController?.abort();
  activePrefetchController = new AbortController();
  const { signal } = activePrefetchController;

  if (router?.prefetch) {
    for (const item of PRODUCT_LINKS) {
      router.prefetch(buildProductHref(item.path, normalized));
    }
  }

  const tasks = PREFETCH_DEFINITIONS
    .map((definition) => {
      const requestUrl = definition.buildUrl(normalized);
      if (!requestUrl) return null;
      return async () => {
        try {
          await loadStableScopeData({
            moduleKey: definition.moduleKey,
            requestUrl,
            scope: normalized,
            signal,
            requestTimeoutMs: definition.requestTimeoutMs,
          });
        } catch (error: any) {
          if (error?.name === 'AbortError') return;
        }
      };
    })
    .filter(Boolean) as Array<() => Promise<void>>;

  void runPrefetchQueue(tasks, 3);
}
