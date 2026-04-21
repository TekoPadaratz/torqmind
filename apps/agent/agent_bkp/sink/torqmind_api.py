from __future__ import annotations

import gzip
from typing import Dict, Iterable, Optional

import requests

from agent_bkp.config import APIConfig, RuntimeConfig
from agent_bkp.spool import SpoolQueue
from agent_bkp.utils.ndjson import to_ndjson_bytes
from agent_bkp.utils.retry import RetryableError, retry_with_backoff


class TorqMindSink:
    def __init__(self, api: APIConfig, runtime: RuntimeConfig, logger) -> None:
        self.api = api
        self.runtime = runtime
        self.logger = logger
        self.session = requests.Session()
        self.spool = SpoolQueue(runtime.spool_dir)

    def _headers(self) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/x-ndjson",
            "Accept": "application/json",
        }
        if self.api.ingest_key:
            headers["X-Ingest-Key"] = self.api.ingest_key
        elif self.api.empresa_id is not None:
            headers["X-Empresa-Id"] = str(self.api.empresa_id)
        else:
            raise ValueError("Missing ingest credentials: set api.ingest_key (prod) or api.empresa_id (dev)")
        return headers

    def check_api(self) -> None:
        url = self.api.base_url.rstrip("/") + "/health"
        self.logger.info("phase=api_health_check url=%s", url)
        resp = self.session.get(url, timeout=self.runtime.timeout_seconds)
        resp.raise_for_status()

    def validate_ingest_credentials(self) -> None:
        self.send(dataset="filiais", rows=[], dry_run=True)

    def flush_spool(self, max_files: Optional[int] = None) -> int:
        max_files = int(max_files or self.runtime.spool_flush_max_files)
        flushed = 0
        for item in self.spool.pending():
            if flushed >= max_files:
                break
            try:
                payload = item.path.read_bytes()
                self._send_payload(dataset=item.dataset, payload=payload, gzip_enabled=item.gzip_enabled, dry_run=False)
                self.spool.remove(item)
                flushed += 1
                self.logger.info("dataset=%s phase=spool_flush_ok file=%s", item.dataset, item.path.name)
            except Exception as exc:  # noqa: PERF203
                self.logger.warning("dataset=%s phase=spool_flush_retry_later file=%s error=%s", item.dataset, item.path.name, str(exc)[:300])
                break
        return flushed

    @staticmethod
    def _retry_after_seconds(resp: requests.Response) -> Optional[float]:
        raw = resp.headers.get("Retry-After")
        if raw is None:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    def _send_payload(self, dataset: str, payload: bytes, gzip_enabled: bool, dry_run: bool) -> Dict:
        headers = self._headers()
        if gzip_enabled:
            headers["Content-Encoding"] = "gzip"

        url = self.api.base_url.rstrip("/") + f"/ingest/{dataset}"
        self.logger.info(
            "dataset=%s phase=send_start url=%s gzip=%s timeout_s=%s",
            dataset,
            url,
            gzip_enabled,
            self.runtime.timeout_seconds,
        )

        def _call():
            resp = self.session.post(url, data=payload, headers=headers, timeout=self.runtime.timeout_seconds)
            if resp.status_code == 429:
                raise RetryableError(
                    f"HTTP 429 {resp.text}",
                    retry_after_seconds=self._retry_after_seconds(resp),
                )
            if 500 <= resp.status_code <= 599:
                raise RetryableError(f"HTTP {resp.status_code} {resp.text}")
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code} {resp.text}")
            return resp

        resp = retry_with_backoff(
            _call,
            max_retries=self.runtime.max_retries,
            on_retry=lambda i, e: self.logger.warning(
                "dataset=%s retry=%s error=%s", dataset, i, str(e)[:300]
            ),
        )

        if dry_run:
            return {"ok": True, "status_code": resp.status_code}

        body = resp.json()
        self.logger.info(
            "dataset=%s phase=send_done status=%s inserted_or_updated=%s rejected=%s",
            dataset,
            resp.status_code,
            body.get("inserted_or_updated"),
            body.get("rejected"),
        )
        return body

    def send(self, dataset: str, rows: Iterable[Dict], dry_run: bool = False) -> Dict:
        flushed = self.flush_spool()
        if flushed:
            self.logger.info("phase=spool_flush_summary files=%s", flushed)

        payload = to_ndjson_bytes(rows)
        if self.runtime.gzip_enabled:
            payload = gzip.compress(payload)

        try:
            return self._send_payload(
                dataset=dataset,
                payload=payload,
                gzip_enabled=self.runtime.gzip_enabled,
                dry_run=dry_run,
            )
        except Exception as exc:  # noqa: PERF203
            if dry_run:
                raise
            item = self.spool.enqueue(dataset=dataset, payload=payload, gzip_enabled=self.runtime.gzip_enabled)
            self.logger.error(
                "dataset=%s phase=spooled file=%s reason=%s suggestion=%s",
                dataset,
                item.path.name,
                str(exc)[:300],
                "validar conectividade API, ingest key e status /health; dados ficaram na fila local",
            )
            return {
                "ok": True,
                "spooled": True,
                "inserted_or_updated": 0,
                "rejected": 0,
                "spool_file": item.path.name,
            }
