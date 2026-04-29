function formatDateOnly(value) {
  if (!value) return null;
  const raw = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return `${raw.slice(8, 10)}/${raw.slice(5, 7)}/${raw.slice(0, 4)}`;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return new Intl.DateTimeFormat('pt-BR', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
  }).format(parsed);
}

function formatDateTimeBr(value) {
  if (!value) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return new Intl.DateTimeFormat('pt-BR', {
    timeZone: 'America/Sao_Paulo',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(parsed).replace(', ', ' ');
}

export function summarizeSnapshotStatus(status) {
  const normalized = String(status || '').toLowerCase();
  if (normalized === 'exact') return 'Base pronta para o período';
  if (normalized === 'best_effort') return 'Base mais recente disponível';
  if (normalized === 'operational_current') return 'Leitura atual do dia';
  if (normalized === 'operational') return 'Base operacional disponível';
  if (normalized === 'missing') return 'Atualizando leitura';
  return 'Base pronta';
}

export function summarizeSourceStatus(status) {
  const normalized = String(status || '').toLowerCase();
  if (normalized === 'ok') return 'Disponível';
  if (normalized === 'partial') return 'Em revisão';
  if (normalized === 'value_gap') return 'Em atualização';
  if (normalized === 'unavailable') return 'Atualizando';
  return 'Disponível';
}

export function describeChurnCoverage(snapshot) {
  const status = String(snapshot?.snapshot_status || 'missing').toLowerCase();
  const effectiveDate = formatDateOnly(snapshot?.effective_dt_ref || snapshot?.requested_dt_ref);

  if (status === 'exact') {
    return `Base de clientes pronta em ${effectiveDate}.`;
  }
  if (status === 'best_effort') {
    return `Usando a base mais recente disponível até ${effectiveDate}.`;
  }
  if (status === 'operational_current') {
    return `Usando a leitura mais recente disponível em ${effectiveDate}.`;
  }
  return 'A leitura de clientes ainda está sendo atualizada para esta data-base.';
}

export function describeFinanceCoverage(aging) {
  const status = String(aging?.snapshot_status || 'missing').toLowerCase();
  const effectiveDate = formatDateOnly(aging?.effective_dt_ref || aging?.dt_ref || aging?.requested_dt_ref);

  if (status === 'exact') {
    return `Financeiro pronto em ${effectiveDate}.`;
  }
  if (status === 'best_effort') {
    return `Mostrando a base financeira mais recente disponível até ${effectiveDate}.`;
  }
  if (status === 'operational') {
    return `Mostrando a leitura financeira mais atual disponível em ${effectiveDate}.`;
  }
  return 'A leitura financeira desta data-base ainda está sendo atualizada.';
}

export function describeCacheBanner(meta, moduleLabel = 'esta tela') {
  if (!meta || !meta.source) return null;
  const normalizedMode = String(meta.mode || '').toLowerCase();
  const fallbackState = String(meta.fallback_state || '').toLowerCase();
  if (normalizedMode === 'fresh_snapshot') return null;
  if (meta.source === 'snapshot') {
    return meta.message || `Mostrando os dados mais recentes de ${moduleLabel} enquanto a atualização termina.`;
  }
  if (meta.source === 'fallback') {
    if (fallbackState === 'operational_current') {
      return meta.message || `Mostrando a leitura atual de ${moduleLabel} enquanto os demais detalhes terminam de fechar.`;
    }
    return meta.message || `Estamos finalizando a atualização de ${moduleLabel}. Os números finais aparecem em instantes.`;
  }
  return null;
}

export function describeCommercialCoverage(coverage, moduleLabel = 'esta tela') {
  const mode = String(coverage?.mode || '').toLowerCase();
  if (!mode || mode === 'exact') return null;

  const latestDate = formatDateOnly(coverage?.latest_available_dt || coverage?.effective_dt_fim);
  const effectiveStart = formatDateOnly(coverage?.effective_dt_ini);
  const effectiveEnd = formatDateOnly(coverage?.effective_dt_fim);

  if (mode === 'shifted_latest') {
    if (effectiveStart && effectiveEnd && effectiveStart !== effectiveEnd) {
      return `A base comercial de ${moduleLabel} ainda vai ate ${latestDate}. Mostrando o ultimo periodo comparavel entre ${effectiveStart} e ${effectiveEnd}.`;
    }
    return `A base comercial de ${moduleLabel} ainda vai ate ${latestDate}. Mostrando a ultima referencia compativel ja publicada.`;
  }

  if (mode === 'partial_requested') {
    return `Os dados de ${moduleLabel} estão disponíveis até ${effectiveEnd}. Os dias posteriores ainda estão chegando.`;
  }

  if (mode === 'missing') {
    return coverage?.message || `Ainda estamos atualizando os dados de ${moduleLabel}.`;
  }

  return coverage?.message || null;
}

export function describeDataFreshness(payload, moduleLabel = 'esta tela') {
  const cacheBanner = describeCacheBanner(payload?._snapshot_cache, moduleLabel);
  if (cacheBanner) return cacheBanner;

  const commercialCoverage = payload?.commercial_coverage
    || payload?.commercial?.commercial_coverage
    || payload?.monthly_projection?.commercial_coverage;
  const coverageBanner = describeCommercialCoverage(
    commercialCoverage,
    moduleLabel,
  );
  if (coverageBanner) return coverageBanner;

  const freshness = payload?.freshness;
  const mode = String(freshness?.mode || '').toLowerCase();
  const liveThrough = formatDateTimeBr(
    freshness?.live_through_at
      || freshness?.sales?.live_through_at
      || freshness?.cash?.live_through_at
      || freshness?.snapshot_generated_at
  );
  const historicalThrough = formatDateOnly(freshness?.historical_through_dt);

  if (mode === 'hybrid_live') {
    if (liveThrough) {
      return `Atualizado em ${liveThrough}.`;
    }
    if (historicalThrough) return `Dados disponíveis até ${historicalThrough}.`;
  }

  if (mode === 'historical_plus_live') {
    if (liveThrough) {
      return `Atualizado em ${liveThrough}.`;
    }
    if (historicalThrough) return `Dados disponíveis até ${historicalThrough}.`;
  }

  if (mode === 'live_monitor' && liveThrough) {
    return `Atualizado em ${liveThrough}.`;
  }

  if (mode === 'hybrid_operational_home' && liveThrough) {
    return `Atualizado em ${liveThrough}.`;
  }

  const publishedAt = formatDateTimeBr(
    freshness?.snapshot_generated_at
      || freshness?.sales?.snapshot_generated_at
      || freshness?.cash?.snapshot_generated_at
  );
  if (publishedAt) {
    return `Atualizado em ${publishedAt}.`;
  }

  const latestCommercialDate = formatDateOnly(
    commercialCoverage?.latest_available_dt
      || commercialCoverage?.effective_dt_fim
      || freshness?.historical_through_dt
  ) || historicalThrough;
  if (latestCommercialDate) {
    return `Dados disponíveis até ${latestCommercialDate}.`;
  }

  return null;
}

export function describeServerBaseDate(value) {
  return formatDateOnly(value) || 'indisponível';
}

export function describeLastSync(syncStatus) {
  const operationalSyncAt = syncStatus?.operational?.last_sync_at || syncStatus?.last_sync_at;
  if (!syncStatus?.available || !operationalSyncAt) {
    const publicationAt = formatDateTimeBr(syncStatus?.publication?.last_sync_at || syncStatus?.analytics?.last_sync_at);
    if (publicationAt) return `Base publicada em ${publicationAt}.`;
    const latestCommercialDate = formatDateOnly(syncStatus?.commercial_coverage?.latest_available_dt);
    if (latestCommercialDate) return `Dados disponíveis até ${latestCommercialDate}.`;
    return 'Estamos atualizando os dados.';
  }
  const formatted = formatDateTimeBr(operationalSyncAt);
  return formatted ? `Atualizado em ${formatted}` : 'Estamos atualizando os dados.';
}

export function describeSyncMessage(syncStatus) {
  const operationalAt = formatDateTimeBr(syncStatus?.operational?.last_sync_at);
  const analyticsAt = formatDateTimeBr(syncStatus?.analytics?.last_sync_at);
  const publicationAt = formatDateTimeBr(syncStatus?.publication?.last_sync_at);

  if (operationalAt && analyticsAt) {
    return `Atualizado em ${analyticsAt}.`;
  }
  if (operationalAt) {
    return `Atualizado em ${operationalAt}.`;
  }
  if (publicationAt) {
    return `Atualizado em ${publicationAt}.`;
  }
  if (analyticsAt) {
    return `Atualizado em ${analyticsAt}.`;
  }
  return syncStatus?.message || 'Dados prontos para consulta.';
}
