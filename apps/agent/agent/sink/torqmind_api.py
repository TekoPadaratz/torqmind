from __future__ import annotations

import gzip
from typing import Dict, Iterable, Optional

import requests

from agent.config import APIConfig, RuntimeConfig
from agent.utils.ndjson import to_ndjson_bytes
from agent.utils.retry import retry_with_backoff


class TorqMindSink:
    def __init__(self, api: APIConfig, runtime: RuntimeConfig, logger) -> None:
        self.api = api
        self.runtime = runtime
        self.logger = logger
        self.session = requests.Session()

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

    def send(self, dataset: str, rows: Iterable[Dict], dry_run: bool = False) -> Dict:
        payload = to_ndjson_bytes(rows)
        headers = self._headers()

        if self.runtime.gzip_enabled:
            payload = gzip.compress(payload)
            headers["Content-Encoding"] = "gzip"

        url = self.api.base_url.rstrip("/") + f"/ingest/{dataset}"
        self.logger.info(
            "dataset=%s phase=send_start url=%s gzip=%s timeout_s=%s",
            dataset,
            url,
            self.runtime.gzip_enabled,
            self.runtime.timeout_seconds,
        )

        def _call():
            resp = self.session.post(url, data=payload, headers=headers, timeout=self.runtime.timeout_seconds)
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
