from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import time
from typing import Iterable, Optional

from agent.config import AppConfig
from agent.extractors.xpert import SQLServerExtractor
from agent.sink.torqmind_api import TorqMindSink
from agent.state.watermark import WatermarkStore


@dataclass
class RunMetrics:
    dataset: str
    extracted: int = 0
    sent: int = 0
    batches: int = 0


class AgentRunner:
    def __init__(self, cfg: AppConfig, logger) -> None:
        self.cfg = cfg
        self.logger = logger
        tenant_key = f"empresa_{cfg.api.empresa_id or cfg.id_empresa or 'unknown'}"
        self.state = WatermarkStore(root_dir=cfg.runtime.state_dir, tenant_key=tenant_key)
        self.extractor = SQLServerExtractor(cfg, logger)
        self.sink = TorqMindSink(cfg.api, cfg.runtime, logger)

    def _enabled_datasets(self) -> Iterable[str]:
        for ds, ds_cfg in self.cfg.datasets.items():
            if ds_cfg.get("enabled", False):
                yield ds

    def check(self) -> None:
        self.logger.info("action=check step=sqlserver")
        self.extractor.check_connection()
        self.logger.info("action=check step=api_health")
        self.sink.check_api()
        self.logger.info("action=check step=ingest_key")
        self.sink.validate_ingest_credentials()
        self.logger.info("action=check result=ok")

    def run_once(
        self,
        only_dataset: Optional[str] = None,
        dt_from: Optional[datetime] = None,
        dt_to: Optional[datetime] = None,
        ignore_watermark: bool = False,
    ) -> None:
        started = time.monotonic()
        try:
            datasets = [only_dataset.lower()] if only_dataset else list(self._enabled_datasets())

            for dataset in datasets:
                t0 = time.monotonic()
                metric = RunMetrics(dataset=dataset)
                scope = f"db:{self.cfg.id_db or 1}"
                watermark = None if ignore_watermark else self.state.get(dataset, scope=scope)
                self.logger.info(
                    "dataset=%s phase=start watermark=%s from=%s to=%s ignore_watermark=%s",
                    dataset,
                    watermark,
                    dt_from,
                    dt_to,
                    ignore_watermark,
                )

                max_watermark_seen = watermark
                for batch in self.extractor.iter_batches(
                    dataset=dataset,
                    watermark=watermark,
                    batch_size=self.cfg.runtime.batch_size,
                    fetch_size=self.cfg.runtime.fetch_size,
                    dt_from=dt_from,
                    dt_to=dt_to,
                ):
                    metric.batches += 1
                    metric.extracted += len(batch.rows)

                    ingest_result = self.sink.send(dataset=dataset, rows=batch.rows)
                    inserted = int(ingest_result.get("inserted_or_updated", 0) or 0)
                    rejected = int(ingest_result.get("rejected", 0) or 0)
                    metric.sent += inserted
                    if metric.extracted > 0 and inserted == 0:
                        raise RuntimeError(
                            f"Batch extracted but nothing inserted for dataset={dataset}. "
                            f"rejected={rejected}. Verify base_url, ingest key and PK fields."
                        )

                    if batch.max_watermark:
                        max_watermark_seen = batch.max_watermark

                    self.logger.info(
                        "dataset=%s phase=batch batches=%s extracted=%s inserted=%s rejected=%s watermark=%s",
                        dataset,
                        metric.batches,
                        metric.extracted,
                        metric.sent,
                        rejected,
                        max_watermark_seen,
                    )

                if max_watermark_seen and not dt_from and not dt_to:
                    self.state.set(dataset, max_watermark_seen, scope=scope)

                self.logger.info(
                    "dataset=%s phase=done extracted=%s sent=%s batches=%s elapsed_s=%.2f",
                    dataset,
                    metric.extracted,
                    metric.sent,
                    metric.batches,
                    time.monotonic() - t0,
                )
        finally:
            self.extractor.close()
            self.logger.info("phase=cycle_done elapsed_s=%.2f", time.monotonic() - started)

    def run_loop(self, interval_seconds: int) -> None:
        while True:
            try:
                self.run_once()
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
        scope = f"db:{self.cfg.id_db or 1}"
        self.state.set(dataset, None, scope=scope)
        self.logger.info("dataset=%s phase=watermark_reset scope=%s", dataset, scope)
