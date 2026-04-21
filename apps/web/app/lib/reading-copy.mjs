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
    return meta.message || `Usando a base mais recente de ${moduleLabel} enquanto o recorte termina de atualizar.`;
  }
  if (meta.source === 'fallback') {
    if (fallbackState === 'operational_current') {
      return meta.message || `Mostrando a leitura atual de ${moduleLabel} enquanto os demais detalhes terminam de fechar.`;
    }
    return meta.message || `Estamos finalizando a atualização de ${moduleLabel}. Os números finais aparecem assim que o recorte estiver pronto.`;
  }
  return null;
}

export function describeDataFreshness(payload, moduleLabel = 'esta tela') {
  const cacheBanner = describeCacheBanner(payload?._snapshot_cache, moduleLabel);
  if (cacheBanner) return cacheBanner;

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
    if (liveThrough && historicalThrough) {
      return `Leitura híbrida ativa em ${moduleLabel}: histórico publicado até ${historicalThrough} e trilho operacional do dia até ${liveThrough}.`;
    }
    if (liveThrough) {
      return `Leitura operacional do dia ativa em ${moduleLabel} até ${liveThrough}.`;
    }
  }

  if (mode === 'historical_plus_live') {
    if (liveThrough && historicalThrough) {
      return `Histórico publicado até ${historicalThrough} e monitor operacional ao vivo até ${liveThrough}.`;
    }
    if (liveThrough) {
      return `Monitor operacional ao vivo até ${liveThrough}.`;
    }
  }

  if (mode === 'live_monitor' && liveThrough) {
    return `Monitor operacional ao vivo em ${moduleLabel} até ${liveThrough}.`;
  }

  if (mode === 'hybrid_operational_home' && liveThrough) {
    return `Dashboard ancorado no trilho operacional até ${liveThrough}, sem esperar o fechamento completo da publicação analítica.`;
  }

  return null;
}

export function describeServerBaseDate(value) {
  return formatDateOnly(value) || 'indisponível';
}

export function describeLastSync(syncStatus) {
  const operationalSyncAt = syncStatus?.operational?.last_sync_at || syncStatus?.last_sync_at;
  if (!syncStatus?.available || !operationalSyncAt) {
    return 'A primeira base pronta ainda está sendo preparada.';
  }
  return formatDateTimeBr(operationalSyncAt) || 'A primeira base pronta ainda está sendo preparada.';
}

export function describeSyncMessage(syncStatus) {
  const operationalAt = formatDateTimeBr(syncStatus?.operational?.last_sync_at);
  const analyticsAt = formatDateTimeBr(syncStatus?.analytics?.last_sync_at);
  const publicationAt = formatDateTimeBr(syncStatus?.publication?.last_sync_at);

  if (operationalAt && analyticsAt) {
    return `Trilho operacional em ${operationalAt}. Publicação analítica mais recente em ${analyticsAt}.`;
  }
  if (operationalAt) {
    return `Trilho operacional em ${operationalAt}.`;
  }
  if (publicationAt) {
    return `Base publicada em ${publicationAt}.`;
  }
  if (analyticsAt) {
    return `Publicação analítica em ${analyticsAt}.`;
  }
  return syncStatus?.message || 'A base mais recente desta empresa já está pronta para consulta.';
}
