const UNSTABLE_CACHE_MODES = new Set([
  'warming_up',
  'protected_unavailable',
  'live_unavailable',
  'protected_stale_snapshot',
]);

export function isScopePayloadStable(payload) {
  if (!payload || typeof payload !== 'object') return false;

  const exactScopeMatch = payload?._snapshot_cache?.exact_scope_match;
  const cacheMode = String(payload?._snapshot_cache?.mode || '').toLowerCase();
  const fallbackState = String(payload?._fallback_meta?.fallback_state || '').toLowerCase();
  const dataState = String(payload?.data_state || payload?._fallback_meta?.data_state || '').toLowerCase();

  if (exactScopeMatch === false) return false;
  if (UNSTABLE_CACHE_MODES.has(cacheMode)) return false;
  if (fallbackState === 'preparing') return false;
  if (dataState === 'transient_unavailable') return false;
  return true;
}

export function buildModuleLoadingCopy(moduleLabel) {
  const label = String(moduleLabel || 'este módulo').trim();
  const scopedLabel = label.startsWith('o ')
    ? `do ${label.slice(2)}`
    : label.startsWith('a ')
      ? `da ${label.slice(2)}`
      : `de ${label}`;
  return {
    headline: `Atualizando a leitura ${scopedLabel}`,
    detail: 'Estamos fechando o novo recorte antes de liberar números e recomendações finais.',
  };
}

export function buildModuleUnavailableCopy(moduleLabel) {
  const label = String(moduleLabel || 'este módulo').trim();
  const scopedLabel = label.startsWith('o ')
    ? `do ${label.slice(2)}`
    : label.startsWith('a ')
      ? `da ${label.slice(2)}`
      : `de ${label}`;
  return {
    headline: `Ainda estamos fechando a leitura ${scopedLabel}`,
    detail: 'Este recorte continua em atualização. Mantivemos a tela protegida para não exibir zero provisório ou dados misturados.',
  };
}
