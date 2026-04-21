from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import time
from typing import Any, Dict, Iterable, Optional

from agent_bkp.config import AppConfig
from agent_bkp.extractors.xpert import SQLServerExtractor
from agent_bkp.sink.torqmind_api import TorqMindSink
from agent_bkp.state.watermark import WatermarkStore
from agent_bkp.utils.log import append_summary_line


@dataclass
class RunMetrics:
    dataset: str
    extracted: int = 0
    sent: int = 0
    rejected: int = 0
    batches: int = 0
    spooled_batches: int = 0
    status: str = "ok"
    error: Optional[str] = None


TURNOS_DATASET = "turnos"
TURNOS_PENDING_STATE_KEY = "turnos_pending"
TURNOS_REVISIT_KEY_COLUMNS = ("ID_FILIAL", "ID_TURNOS")
TURNOS_REVISIT_QUERY_CHUNK = 200


class AgentRunner:
    def __init__(self, cfg: AppConfig, logger) -> None:
        self.cfg = cfg
        self.logger = logger
        tenant_key = f"empresa_{cfg.api.empresa_id or cfg.id_empresa or 'unknown'}"
        self.state = WatermarkStore(root_dir=cfg.runtime.state_dir, tenant_key=tenant_key)
        self.extractor = SQLServerExtractor(cfg, logger)
        self.sink = TorqMindSink(cfg.api, cfg.runtime, logger)
        self._log_startup_summary()

    def _log_startup_summary(self) -> None:
        enabled = list(self._enabled_datasets())
        self.logger.info(
            "phase=startup api_base_url=%s api_route_prefix=%s state_dir=%s spool_dir=%s connect_timeout_s=%s read_timeout_s=%s max_retries=%s backoff_base_s=%s backoff_max_s=%s summary_log_file=%s datasets_enabled=%s",
            self.cfg.api.base_url,
            self.cfg.api.route_prefix,
            self.cfg.runtime.state_dir,
            self.cfg.runtime.spool_dir,
            self.cfg.runtime.effective_connect_timeout_seconds,
            self.cfg.runtime.effective_read_timeout_seconds,
            self.cfg.runtime.max_retries,
            self.cfg.runtime.retry_backoff_base_seconds,
            self.cfg.runtime.retry_backoff_max_seconds,
            self.cfg.runtime.summary_log_file,
            ",".join(enabled) if enabled else "<none>",
        )

    def _write_cycle_summary(self, metrics: list[RunMetrics], *, started: float) -> None:
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        spool = self.sink.spool_status()
        ok = sum(1 for m in metrics if m.status == "ok")
        failed = sum(1 for m in metrics if m.status == "failed")

        append_summary_line(
            self.cfg.runtime.summary_log_file,
            (
                f"{now_iso} cycle datasets_total={len(metrics)} datasets_ok={ok} datasets_failed={failed} "
                f"pending_spool={spool.get('pending_files', 0)} dead_letter={spool.get('dead_letter_files', 0)} "
                f"elapsed_s={time.monotonic() - started:.2f}"
            ),
        )
        for metric in metrics:
            append_summary_line(
                self.cfg.runtime.summary_log_file,
                (
                    f"{now_iso} dataset={metric.dataset} status={metric.status} extracted={metric.extracted} "
                    f"sent={metric.sent} rejected={metric.rejected} spooled_batches={metric.spooled_batches} "
                    f"batches={metric.batches} error={metric.error or ''}"
                ),
            )

    def _enabled_datasets(self) -> Iterable[str]:
        for ds, ds_cfg in self.cfg.datasets.items():
            if ds_cfg.get("enabled", False):
                yield ds

    def _scope(self) -> str:
        return f"db:{self.cfg.id_db or 1}"

    @staticmethod
    def _dataset_days(ds_cfg: dict, key: str) -> Optional[int]:
        value = ds_cfg.get(key)
        if value in {None, ""}:
            return None
        return int(value)

    @staticmethod
    def _rolling_window(days: int, now: Optional[datetime] = None) -> tuple[datetime, datetime]:
        anchor = now or datetime.now()
        window_end = datetime.combine((anchor + timedelta(days=1)).date(), datetime.min.time())
        window_start = window_end - timedelta(days=int(days))
        return window_start, window_end

    def _bootstrap_complete(self, dataset: str, scope: str, watermark_before: Optional[str]) -> bool:
        if self.state.is_bootstrap_complete(dataset, scope=scope):
            return True
        if watermark_before:
            self.state.mark_bootstrap_complete(dataset, scope=scope)
            self.logger.info(
                "dataset=%s phase=bootstrap_state_reused scope=%s reason=existing_watermark",
                dataset,
                scope,
            )
            return True
        return False

    def _resolve_dataset_window(
        self,
        dataset: str,
        scope: str,
        watermark_before: Optional[str],
        requested_dt_from: Optional[datetime],
        requested_dt_to: Optional[datetime],
        ignore_watermark: bool,
    ) -> tuple[Optional[datetime], Optional[datetime], bool, str, bool]:
        if requested_dt_from is not None or requested_dt_to is not None:
            return requested_dt_from, requested_dt_to, ignore_watermark, "manual", False

        ds_cfg = self.cfg.datasets.get(dataset, {})
        if ds_cfg.get("full_refresh"):
            return None, None, True, "full_refresh", False
        retention_days = self._dataset_days(ds_cfg, "retention_days")
        bootstrap_days = self._dataset_days(ds_cfg, "bootstrap_days")
        if retention_days is None and bootstrap_days is None:
            return None, None, ignore_watermark, "default", False
        bootstrap_complete = self._bootstrap_complete(dataset, scope, watermark_before)

        if bootstrap_days and not bootstrap_complete:
            dt_from, dt_to = self._rolling_window(bootstrap_days)
            return dt_from, dt_to, True, "bootstrap", True

        if retention_days:
            dt_from, dt_to = self._rolling_window(retention_days)
            return dt_from, dt_to, ignore_watermark, "retention", False

        return None, None, ignore_watermark, "default", False

    @staticmethod
    def _is_newer_watermark(candidate: Optional[str], current: Optional[str]) -> bool:
        if candidate is None:
            return False
        if current is None:
            return True

        cand_dt = WatermarkStore.parse_watermark_dt(candidate)
        curr_dt = WatermarkStore.parse_watermark_dt(current)
        if cand_dt and curr_dt:
            try:
                return cand_dt > curr_dt
            except TypeError:
                # Fallback for mixed aware/naive datetimes.
                return cand_dt.replace(tzinfo=None) > curr_dt.replace(tzinfo=None)
        return str(candidate) > str(current)

    @staticmethod
    def _chunked(items: list[Any], size: int) -> Iterable[list[Any]]:
        chunk_size = max(1, int(size))
        for idx in range(0, len(items), chunk_size):
            yield items[idx : idx + chunk_size]

    @staticmethod
    def _allow_zero_inserted(ds_cfg: dict) -> bool:
        return bool(ds_cfg.get("allow_zero_inserted_batches", False) or ds_cfg.get("full_refresh", False))

    def _validate_batch_delivery(
        self,
        *,
        dataset: str,
        ds_cfg: dict,
        extracted: int,
        inserted: int,
        rejected: int,
        spooled: bool,
    ) -> None:
        if extracted > 0 and inserted == 0 and not spooled and not self._allow_zero_inserted(ds_cfg):
            raise RuntimeError(
                f"Batch extracted but nothing inserted for dataset={dataset}. "
                f"rejected={rejected}. Verify base_url, ingest key and PK fields."
            )

    @staticmethod
    def _serialize_state_value(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat(timespec="microseconds")
        return str(value)

    @staticmethod
    def _normalize_optional_int(value: Any) -> Any:
        if value in {None, ""}:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return value

    @staticmethod
    def _turno_key(id_filial: Any, id_turnos: Any) -> Optional[str]:
        filial = AgentRunner._normalize_optional_int(id_filial)
        turno = AgentRunner._normalize_optional_int(id_turnos)
        if filial in {None, ""} or turno in {None, ""}:
            return None
        return f"{filial}:{turno}"

    def _turno_key_from_row(self, row: Dict[str, Any]) -> Optional[str]:
        return self._turno_key(row.get("ID_FILIAL"), row.get("ID_TURNOS"))

    @staticmethod
    def _turno_is_closed(row: Dict[str, Any]) -> bool:
        value = row.get("ENCERRANTEFECHAMENTO")
        if value in {None, ""}:
            return False
        try:
            return int(value) != 0
        except (TypeError, ValueError):
            return str(value).strip().lower() not in {"", "0", "0.0", "false", "none"}

    def _turno_row_watermark(self, row: Dict[str, Any]) -> Optional[str]:
        for key in ("TORQMIND_WATERMARK", "DATAFECHAMENTO", "TORQMIND_DT_EVENTO", "DATA", "DATATURNO", "DATAREPL"):
            value = self._serialize_state_value(row.get(key))
            if value not in {None, ""}:
                return value
        return None

    def _load_turnos_pending(self, scope: str) -> Dict[str, Dict[str, Any]]:
        raw = self.state.get_scope_value(TURNOS_DATASET, TURNOS_PENDING_STATE_KEY, scope=scope, default={})
        if not isinstance(raw, dict):
            return {}
        return {str(key): dict(value or {}) for key, value in raw.items()}

    def _save_turnos_pending(self, scope: str, pending: Dict[str, Dict[str, Any]]) -> None:
        if pending:
            self.state.set_scope_value(TURNOS_DATASET, TURNOS_PENDING_STATE_KEY, pending, scope=scope)
        else:
            self.state.clear_scope_value(TURNOS_DATASET, TURNOS_PENDING_STATE_KEY, scope=scope)

    def _turnos_status_payload(self, scope: str) -> Dict[str, Any]:
        pending = self._load_turnos_pending(scope)
        items = sorted(
            pending.values(),
            key=lambda item: (
                str(item.get("last_seen_at") or ""),
                int(item.get("id_filial") or 0),
                int(item.get("id_turnos") or 0),
            ),
            reverse=True,
        )
        open_count = sum(1 for item in items if item.get("last_known_status") == "open")
        closed_count = sum(1 for item in items if item.get("last_known_status") == "closed")
        pending_attempts = sum(int(item.get("final_close_attempts") or 0) for item in items if item.get("last_known_status") == "closed")
        return {
            "scope": scope,
            "pending_total": len(items),
            "open_pending": open_count,
            "closed_pending": closed_count,
            "closed_pending_attempts": pending_attempts,
            "items": items,
        }

    def turnos_status(self) -> Dict[str, Any]:
        return self._turnos_status_payload(self._scope())

    def _record_turnos_delivery(
        self,
        *,
        pending: Dict[str, Dict[str, Any]],
        rows: list[Dict[str, Any]],
        scope: str,
        spooled: bool,
        phase: str,
        seen_keys: set[str],
    ) -> Dict[str, int]:
        now_iso = datetime.now(timezone.utc).isoformat()
        changed = False
        stats = {
            "tracked_open": 0,
            "tracked_closed": 0,
            "removed_closed": 0,
            "untracked": 0,
        }

        for row in rows:
            key = self._turno_key_from_row(row)
            if key is None:
                stats["untracked"] += 1
                continue
            seen_keys.add(key)
            is_closed = self._turno_is_closed(row)
            current = dict(pending.get(key) or {})
            final_close_attempts = int(current.get("final_close_attempts") or 0)
            entry: Dict[str, Any] = {
                "id_filial": self._normalize_optional_int(row.get("ID_FILIAL")),
                "id_turnos": self._normalize_optional_int(row.get("ID_TURNOS")),
                "id_usuarios": self._normalize_optional_int(row.get("ID_USUARIOS")),
                "encerrante_fechamento": self._normalize_optional_int(row.get("ENCERRANTEFECHAMENTO")),
                "first_seen_at": current.get("first_seen_at") or now_iso,
                "last_seen_at": now_iso,
                "last_known_status": "closed" if is_closed else "open",
                "last_sent_at": now_iso,
                "last_sent_phase": phase,
                "last_delivery": "spooled" if spooled else "direct",
                "last_source_watermark": self._turno_row_watermark(row),
                "missing_count": 0,
                "last_missing_at": None,
                "final_close_attempts": final_close_attempts,
                "final_close_confirmed": bool(current.get("final_close_confirmed")),
            }

            if is_closed:
                entry["final_close_attempts"] = final_close_attempts + 1
                entry["final_close_confirmed"] = False if spooled else True
                entry["close_pending"] = True
            else:
                entry["final_close_attempts"] = 0
                entry["final_close_confirmed"] = False
                entry["close_pending"] = False

            if is_closed and entry["final_close_confirmed"]:
                if key in pending:
                    pending.pop(key, None)
                    changed = True
                stats["removed_closed"] += 1
                continue

            pending[key] = entry
            changed = True
            if is_closed:
                stats["tracked_closed"] += 1
            else:
                stats["tracked_open"] += 1

        if changed:
            self._save_turnos_pending(scope, pending)
        return stats

    def _mark_missing_turnos(
        self,
        *,
        pending: Dict[str, Dict[str, Any]],
        scope: str,
        requested_keys: list[str],
        returned_rows: list[Dict[str, Any]],
    ) -> int:
        returned_keys = {key for key in (self._turno_key_from_row(row) for row in returned_rows) if key}
        missing_keys = [key for key in requested_keys if key not in returned_keys]
        if not missing_keys:
            return 0

        now_iso = datetime.now(timezone.utc).isoformat()
        changed = False
        for key in missing_keys:
            current = dict(pending.get(key) or {})
            if not current:
                continue
            current["last_missing_at"] = now_iso
            current["missing_count"] = int(current.get("missing_count", 0) or 0) + 1
            pending[key] = current
            changed = True

        if changed:
            self._save_turnos_pending(scope, pending)

        self.logger.warning(
            "dataset=%s phase=revisit_missing requested=%s missing=%s keys=%s",
            TURNOS_DATASET,
            len(requested_keys),
            len(missing_keys),
            ",".join(missing_keys[:20]),
        )
        return len(missing_keys)

    def _revisit_pending_turnos(
        self,
        *,
        scope: str,
        pending: Dict[str, Dict[str, Any]],
        seen_keys: set[str],
        metric: RunMetrics,
        ds_cfg: dict,
    ) -> None:
        revisit_entries = [item for key, item in pending.items() if key not in seen_keys]
        if not revisit_entries:
            self.logger.info("dataset=%s phase=revisit_skip pending=0", TURNOS_DATASET)
            return

        self.logger.info(
            "dataset=%s phase=revisit_start pending=%s already_seen=%s",
            TURNOS_DATASET,
            len(revisit_entries),
            len(seen_keys),
        )

        for key_chunk in self._chunked(revisit_entries, TURNOS_REVISIT_QUERY_CHUNK):
            lookup_keys = []
            requested_keys = []
            for item in key_chunk:
                key = self._turno_key(item.get("id_filial"), item.get("id_turnos"))
                if key is None:
                    continue
                lookup_keys.append(
                    {
                        "ID_FILIAL": item.get("id_filial"),
                        "ID_TURNOS": item.get("id_turnos"),
                    }
                )
                requested_keys.append(key)

            if not lookup_keys:
                continue

            rows = self.extractor.fetch_rows_by_keys(
                TURNOS_DATASET,
                key_columns=list(TURNOS_REVISIT_KEY_COLUMNS),
                keys=lookup_keys,
                fetch_size=self.cfg.runtime.fetch_size,
                query_chunk_size=TURNOS_REVISIT_QUERY_CHUNK,
            )
            self._mark_missing_turnos(
                pending=pending,
                scope=scope,
                requested_keys=requested_keys,
                returned_rows=rows,
            )
            if not rows:
                continue

            for batch_rows in self._chunked(rows, self.cfg.runtime.batch_size):
                metric.batches += 1
                metric.extracted += len(batch_rows)

                ingest_result = self.sink.send(dataset=TURNOS_DATASET, rows=batch_rows)
                inserted = int(ingest_result.get("inserted_or_updated", 0) or 0)
                rejected = int(ingest_result.get("rejected", 0) or 0)
                spooled = bool(ingest_result.get("spooled", False))
                metric.sent += inserted
                metric.rejected += rejected
                if spooled:
                    metric.spooled_batches += 1

                self._validate_batch_delivery(
                    dataset=TURNOS_DATASET,
                    ds_cfg=ds_cfg,
                    extracted=len(batch_rows),
                    inserted=inserted,
                    rejected=rejected,
                    spooled=spooled,
                )
                stats = self._record_turnos_delivery(
                    pending=pending,
                    rows=batch_rows,
                    scope=scope,
                    spooled=spooled,
                    phase="revisit",
                    seen_keys=seen_keys,
                )
                self.logger.info(
                    "dataset=%s phase=revisit_batch rows=%s inserted=%s rejected=%s spooled=%s tracked_open=%s tracked_closed=%s removed_closed=%s untracked=%s",
                    TURNOS_DATASET,
                    len(batch_rows),
                    inserted,
                    rejected,
                    spooled,
                    stats["tracked_open"],
                    stats["tracked_closed"],
                    stats["removed_closed"],
                    stats["untracked"],
                )

        status = self._turnos_status_payload(scope)
        self.logger.info(
            "dataset=%s phase=revisit_done pending_total=%s open_pending=%s closed_pending=%s",
            TURNOS_DATASET,
            status["pending_total"],
            status["open_pending"],
            status["closed_pending"],
        )

    def check(self) -> None:
        self.logger.info(
            "action=check step=sqlserver server=%s driver=%s database=%s",
            self.cfg.sqlserver.server,
            self.cfg.sqlserver.driver,
            self.cfg.sqlserver.database,
        )
        self.extractor.check_connection()
        self.logger.info("action=check step=api_ping")
        self.sink.check_api()
        self.logger.info("action=check step=ingest_auth")
        self.sink.validate_ingest_credentials()
        self.logger.info("action=check step=ingest_post")
        self.sink.test_ingest_endpoint(dataset="filiais")
        self.logger.info("action=check result=ok")

    def run_once(
        self,
        only_dataset: Optional[str] = None,
        dt_from: Optional[datetime] = None,
        dt_to: Optional[datetime] = None,
        ignore_watermark: bool = False,
        continue_on_error: bool = False,
    ) -> None:
        started = time.monotonic()
        metrics: list[RunMetrics] = []
        manual_window = dt_from is not None or dt_to is not None
        try:
            flushed_at_start = self.sink.flush_spool()
            if flushed_at_start:
                self.logger.info("phase=spool_flush_startup files=%s", flushed_at_start)

            datasets = [only_dataset.lower()] if only_dataset else list(self._enabled_datasets())

            for dataset in datasets:
                t0 = time.monotonic()
                metric = RunMetrics(dataset=dataset)
                metrics.append(metric)
                scope = self._scope()
                ds_cfg = self.cfg.datasets.get(dataset, {})
                turnos_pending = self._load_turnos_pending(scope) if dataset == TURNOS_DATASET else {}
                turnos_seen_keys: set[str] = set()
                full_refresh = bool(ds_cfg.get("full_refresh", False))
                watermark_before = None if (ignore_watermark or full_refresh) else self.state.get(dataset, scope=scope)
                effective_dt_from, effective_dt_to, effective_ignore_watermark, window_mode, bootstrap_run = self._resolve_dataset_window(
                    dataset,
                    scope,
                    watermark_before,
                    dt_from,
                    dt_to,
                    (ignore_watermark or full_refresh),
                )
                effective_watermark = None if effective_ignore_watermark else watermark_before
                commit_watermark = (not manual_window) and (not full_refresh)
                self.logger.info(
                    "dataset=%s phase=start mode=%s full_refresh=%s watermark=%s effective_watermark=%s from=%s to=%s ignore_watermark=%s effective_ignore_watermark=%s",
                    dataset,
                    window_mode,
                    full_refresh,
                    watermark_before,
                    effective_watermark,
                    effective_dt_from,
                    effective_dt_to,
                    ignore_watermark,
                    effective_ignore_watermark,
                )

                max_watermark_seen = effective_watermark
                # Keep one watermark step lag before committing to avoid skipping rows
                # that share the same watermark across batch boundaries.
                committed_watermark = effective_watermark
                pending_watermark = effective_watermark
                try:
                    for batch in self.extractor.iter_batches(
                        dataset=dataset,
                        watermark=effective_watermark,
                        batch_size=self.cfg.runtime.batch_size,
                        fetch_size=self.cfg.runtime.fetch_size,
                        dt_from=effective_dt_from,
                        dt_to=effective_dt_to,
                    ):
                        metric.batches += 1
                        metric.extracted += len(batch.rows)

                        ingest_result = self.sink.send(dataset=dataset, rows=batch.rows)
                        inserted = int(ingest_result.get("inserted_or_updated", 0) or 0)
                        rejected = int(ingest_result.get("rejected", 0) or 0)
                        spooled = bool(ingest_result.get("spooled", False))
                        metric.sent += inserted
                        metric.rejected += rejected
                        if spooled:
                            metric.spooled_batches += 1
                        self._validate_batch_delivery(
                            dataset=dataset,
                            ds_cfg=ds_cfg,
                            extracted=len(batch.rows),
                            inserted=inserted,
                            rejected=rejected,
                            spooled=spooled,
                        )
                        if dataset == TURNOS_DATASET:
                            stats = self._record_turnos_delivery(
                                pending=turnos_pending,
                                rows=batch.rows,
                                scope=scope,
                                spooled=spooled,
                                phase="incremental",
                                seen_keys=turnos_seen_keys,
                            )
                            self.logger.info(
                                "dataset=%s phase=turnos_tracking tracked_open=%s tracked_closed=%s removed_closed=%s untracked=%s pending_total=%s",
                                dataset,
                                stats["tracked_open"],
                                stats["tracked_closed"],
                                stats["removed_closed"],
                                stats["untracked"],
                                len(turnos_pending),
                            )

                        if batch.max_watermark:
                            max_watermark_seen = batch.max_watermark
                            if self._is_newer_watermark(batch.max_watermark, pending_watermark):
                                if (
                                    commit_watermark
                                    and pending_watermark
                                    and self._is_newer_watermark(pending_watermark, committed_watermark)
                                ):
                                    self.state.set(dataset, pending_watermark, scope=scope)
                                    committed_watermark = pending_watermark
                                    self.logger.info(
                                        "dataset=%s phase=checkpoint watermark=%s",
                                        dataset,
                                        committed_watermark,
                                    )
                                pending_watermark = batch.max_watermark

                        self.logger.info(
                            "dataset=%s phase=batch batches=%s extracted=%s inserted=%s rejected=%s spooled=%s watermark=%s",
                            dataset,
                            metric.batches,
                            metric.extracted,
                            metric.sent,
                            rejected,
                            spooled,
                            max_watermark_seen,
                        )

                    if dataset == TURNOS_DATASET:
                        self._revisit_pending_turnos(
                            scope=scope,
                            pending=turnos_pending,
                            seen_keys=turnos_seen_keys,
                            metric=metric,
                            ds_cfg=ds_cfg,
                        )

                    if (
                        commit_watermark
                        and pending_watermark
                        and self._is_newer_watermark(pending_watermark, committed_watermark)
                    ):
                        self.state.set(dataset, pending_watermark, scope=scope)
                        committed_watermark = pending_watermark
                        self.logger.info(
                            "dataset=%s phase=checkpoint watermark=%s",
                            dataset,
                            committed_watermark,
                        )
                    watermark_after = self.state.get(dataset, scope=scope)

                    self.logger.info(
                        "dataset=%s phase=done mode=%s extracted=%s sent=%s batches=%s elapsed_s=%.2f watermark_before=%s watermark_after=%s",
                        dataset,
                        window_mode,
                        metric.extracted,
                        metric.sent,
                        metric.batches,
                        time.monotonic() - t0,
                        watermark_before,
                        watermark_after,
                    )
                    if bootstrap_run:
                        self.state.mark_bootstrap_complete(dataset, scope=scope)
                        self.logger.info(
                            "dataset=%s phase=bootstrap_complete scope=%s watermark_after=%s",
                            dataset,
                            scope,
                            watermark_after,
                        )
                except Exception as exc:  # noqa: PERF203
                    metric.status = "failed"
                    metric.error = str(exc)
                    self.logger.exception(
                        "dataset=%s phase=failed elapsed_s=%.2f error=%s",
                        dataset,
                        time.monotonic() - t0,
                        str(exc),
                    )
                    if not continue_on_error:
                        raise
        finally:
            self.extractor.close()
            ok = sum(1 for m in metrics if m.status == "ok")
            failed = sum(1 for m in metrics if m.status == "failed")
            self.logger.info("phase=summary datasets_total=%s datasets_ok=%s datasets_failed=%s", len(metrics), ok, failed)
            spool = self.sink.spool_status()
            self.logger.info(
                "phase=summary_spool pending_files=%s pending_rows=%s dead_letter_files=%s",
                spool.get("pending_files", 0),
                spool.get("pending_rows", 0),
                spool.get("dead_letter_files", 0),
            )
            for m in metrics:
                self.logger.info(
                    "phase=summary_dataset dataset=%s status=%s extracted=%s sent=%s rejected=%s spooled_batches=%s batches=%s error=%s",
                    m.dataset,
                    m.status,
                    m.extracted,
                    m.sent,
                    m.rejected,
                    m.spooled_batches,
                    m.batches,
                    m.error,
                )
            self.logger.info("phase=cycle_done elapsed_s=%.2f", time.monotonic() - started)
            self._write_cycle_summary(metrics, started=started)

    def run_loop(
        self,
        interval_seconds: int,
        *,
        only_dataset: Optional[str] = None,
        continue_on_error: bool = False,
    ) -> None:
        while True:
            try:
                self.run_once(only_dataset=only_dataset, continue_on_error=continue_on_error)
            except Exception as exc:  # noqa: PERF203
                self.logger.exception("phase=loop_error error=%s", str(exc)[:500])
            time.sleep(interval_seconds)

    def backfill(self, dataset: str, from_date: datetime, to_date: datetime) -> None:
        # to_date is inclusive in CLI; convert to exclusive upper bound for query.
        to_exclusive = to_date + timedelta(days=1)
        self.run_once(
            only_dataset=dataset,
            dt_from=from_date,
            dt_to=to_exclusive,
            ignore_watermark=True,
        )

    def reset_watermark(self, dataset: str) -> None:
        scope = self._scope()
        self.state.set(dataset, None, scope=scope)
        self.state.clear_bootstrap(dataset, scope=scope)
        if dataset.lower() == TURNOS_DATASET:
            self.state.clear_scope_value(TURNOS_DATASET, TURNOS_PENDING_STATE_KEY, scope=scope)
            self.logger.info("dataset=%s phase=revisit_state_reset scope=%s", dataset, scope)
        self.logger.info("dataset=%s phase=watermark_reset scope=%s bootstrap_cleared=true", dataset, scope)

    def schema_scan(self, keywords: list[str]) -> dict:
        self.logger.info("phase=schema_scan_start keywords=%s", keywords)
        report = self.extractor.schema_scan(keywords=keywords)
        self.extractor.close()
        self.logger.info("phase=schema_scan_done candidates=%s", len(report.get("candidates", [])))
        return report
