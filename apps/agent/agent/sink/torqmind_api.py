from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
from typing import Dict, Iterable, Optional

import requests

from agent.config import APIConfig, RuntimeConfig
from agent.spool import SpoolQueue
from agent.utils.ndjson import to_ndjson_bytes
from agent.utils.retry import RetryableError, retry_with_backoff


class SinkError(RuntimeError):
    pass


class PermanentTransportError(SinkError):
    pass


class PermanentPayloadError(SinkError):
    pass


class TorqMindSink:
    def __init__(self, api: APIConfig, runtime: RuntimeConfig, logger) -> None:
        self.api = api
        self.runtime = runtime
        self.logger = logger
        self.session = requests.Session()
        self.spool = SpoolQueue(runtime.spool_dir)
        self._resolved_api_root: Optional[str] = None

    def _timeout(self) -> tuple[int, int]:
        return self.runtime.request_timeout

    def _candidate_api_roots(self) -> list[str]:
        base = str(self.api.base_url or "").strip().rstrip("/")
        if not base:
            raise ValueError("Missing api.base_url")

        configured_prefix = str(self.api.route_prefix or "auto").strip()
        candidates: list[str] = []
        if configured_prefix and configured_prefix.lower() != "auto":
            prefix = configured_prefix if configured_prefix.startswith("/") else f"/{configured_prefix}"
            candidates.append((base + prefix).rstrip("/"))
        else:
            candidates.append(base)
            if not base.lower().endswith("/api"):
                candidates.append(f"{base}/api")

        ordered: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            normalized = candidate.rstrip("/")
            if normalized and normalized not in seen:
                ordered.append(normalized)
                seen.add(normalized)
        return ordered

    def _request(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        data: bytes | None = None,
    ) -> requests.Response:
        return self.session.request(
            method=method,
            url=url,
            headers=headers,
            data=data,
            timeout=self._timeout(),
            allow_redirects=False,
        )

    @staticmethod
    def _retry_after_seconds(resp: requests.Response) -> Optional[float]:
        raw = resp.headers.get("Retry-After")
        if raw is None:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    @staticmethod
    def _body_text(resp: requests.Response) -> str:
        try:
            if resp.content:
                return resp.text.strip()
        except Exception:
            return ""
        return ""

    @staticmethod
    def _body_json(resp: requests.Response) -> Dict:
        if not resp.content:
            return {}
        try:
            body = resp.json()
            return body if isinstance(body, dict) else {"data": body}
        except ValueError:
            return {"raw": resp.text[:1000]}

    @staticmethod
    def _delivery_key(dataset: str, payload: bytes) -> str:
        digest = hashlib.sha256(payload).hexdigest()
        return f"{dataset.strip().lower()}-{digest}"

    def _headers(
        self,
        *,
        include_auth: bool = True,
        include_content_type: bool = True,
        idempotency_key: str = "",
    ) -> Dict[str, str]:
        headers = {
            "Accept": "application/json",
        }
        if include_content_type:
            headers["Content-Type"] = "application/x-ndjson"

        if include_auth:
            if self.api.ingest_key:
                headers["X-Ingest-Key"] = self.api.ingest_key
            elif self.api.empresa_id is not None:
                headers["X-Empresa-Id"] = str(self.api.empresa_id)
            else:
                raise ValueError("Missing ingest credentials: set api.ingest_key (prod) or api.empresa_id (legacy/dev)")

        if idempotency_key and self.api.idempotency_header:
            headers[self.api.idempotency_header] = idempotency_key

        return headers

    def _probe_api_root(self, candidate: str) -> tuple[bool, str]:
        url = candidate.rstrip("/") + "/ingest/health"
        try:
            resp = self._request(
                "GET",
                url,
                headers=self._headers(include_auth=False, include_content_type=False),
            )
        except requests.RequestException as exc:
            return False, f"request_exception:{exc.__class__.__name__}:{str(exc)}"

        content_type = str(resp.headers.get("Content-Type", "")).lower()
        if resp.status_code == 404:
            return False, "not_found"
        if resp.status_code in {200, 401, 403, 405, 422} or "application/json" in content_type:
            return True, f"status:{resp.status_code}"
        return False, f"status:{resp.status_code}"

    def resolve_api_root(self, force: bool = False) -> str:
        if self._resolved_api_root and not force:
            return self._resolved_api_root

        candidates = self._candidate_api_roots()
        probe_failures: list[str] = []
        had_network_error = False
        for candidate in candidates:
            ok, reason = self._probe_api_root(candidate)
            if ok:
                self._resolved_api_root = candidate
                self.logger.info("phase=api_root_resolved public_base_url=%s resolved_api_root=%s", self.api.base_url, candidate)
                return candidate
            probe_failures.append(f"{candidate} ({reason})")
            if str(reason).startswith("request_exception:"):
                had_network_error = True

        if had_network_error:
            raise RetryableError(
                f"Unable to reach ingest API root from api.base_url={self.api.base_url}. "
                f"Tried: {', '.join(probe_failures)}"
            )

        raise PermanentTransportError(
            f"Unable to resolve ingest API root from api.base_url={self.api.base_url}. "
            f"Tried: {', '.join(probe_failures or candidates)}"
        )

    def _url(self, path: str) -> str:
        return self.resolve_api_root().rstrip("/") + path

    def _raise_for_response(self, resp: requests.Response, *, dataset: str | None = None, url: str = "") -> None:
        status = int(resp.status_code)
        body_text = self._body_text(resp)
        summary = body_text[:400] if body_text else "<empty>"

        if status == 429:
            raise RetryableError(
                f"HTTP 429 dataset={dataset or 'n/a'} url={url} body={summary}",
                retry_after_seconds=self._retry_after_seconds(resp),
            )

        if status in {408, 500, 502, 503, 504}:
            lowered = body_text.lower()
            if "invalid input syntax for type uuid" in lowered:
                raise PermanentTransportError(
                    "Invalid X-Ingest-Key format. The published API expects a UUID in X-Ingest-Key."
                )
            raise RetryableError(f"HTTP {status} dataset={dataset or 'n/a'} url={url} body={summary}")

        if status in {401, 403}:
            raise PermanentTransportError(
                f"Authentication failed for dataset={dataset or 'n/a'} url={url}. "
                f"HTTP {status} body={summary}"
            )

        if status in {404, 405}:
            raise PermanentTransportError(
                f"Ingest endpoint not available for dataset={dataset or 'n/a'} url={url}. "
                f"HTTP {status} body={summary}"
            )

        if status in {400, 409, 413, 415, 422}:
            raise PermanentPayloadError(
                f"Payload rejected for dataset={dataset or 'n/a'} url={url}. "
                f"HTTP {status} body={summary}"
            )

        if status >= 400:
            raise PermanentTransportError(
                f"Unexpected HTTP failure for dataset={dataset or 'n/a'} url={url}. "
                f"HTTP {status} body={summary}"
            )

    def ping(self) -> Dict:
        public_health_url = str(self.api.base_url).rstrip("/") + "/health"
        public_health = self._request("GET", public_health_url, headers=self._headers(include_auth=False, include_content_type=False))
        if public_health.status_code >= 400:
            self._raise_for_response(public_health, url=public_health_url)

        resolved_api_root = self.resolve_api_root(force=True)
        resolved_health_url = resolved_api_root.rstrip("/") + "/health"
        ingest_health_url = resolved_api_root.rstrip("/") + "/ingest/health"
        resolved_health = self._request(
            "GET",
            resolved_health_url,
            headers=self._headers(include_auth=False, include_content_type=False),
        )
        if resolved_health.status_code >= 400:
            self._raise_for_response(resolved_health, url=resolved_health_url)

        ingest_health = self._request(
            "GET",
            ingest_health_url,
            headers=self._headers(include_auth=False, include_content_type=False),
        )

        result = {
            "public_base_url": str(self.api.base_url).rstrip("/"),
            "resolved_api_root": resolved_api_root,
            "public_health_url": public_health_url,
            "public_health_status": int(public_health.status_code),
            "resolved_health_url": resolved_health_url,
            "resolved_health_status": int(resolved_health.status_code),
            "ingest_health_url": ingest_health_url,
            "ingest_health_status": int(ingest_health.status_code),
            "ingest_health_body": self._body_text(ingest_health)[:300],
        }
        self.logger.info(
            "phase=api_ping public_health_status=%s resolved_health_status=%s ingest_health_status=%s resolved_api_root=%s",
            result["public_health_status"],
            result["resolved_health_status"],
            result["ingest_health_status"],
            result["resolved_api_root"],
        )
        return result

    def check_api(self) -> None:
        result = self.ping()
        if int(result["resolved_health_status"]) >= 400:
            raise PermanentTransportError(
                f"API health failed at {result['resolved_health_url']} with HTTP {result['resolved_health_status']}"
            )

    def validate_ingest_credentials(self) -> Dict:
        url = self._url("/ingest/health")
        self.logger.info("phase=ingest_auth_check url=%s", url)
        resp = self._request("GET", url, headers=self._headers(include_auth=True, include_content_type=False))
        if resp.status_code >= 400:
            self._raise_for_response(resp, url=url)
        body = self._body_json(resp)
        self.logger.info("phase=ingest_auth_ok url=%s status=%s", url, resp.status_code)
        return {"status_code": int(resp.status_code), "body": body}

    def test_ingest_endpoint(self, dataset: str = "filiais") -> Dict:
        gzip_enabled = bool(self.runtime.gzip_enabled)
        payload = b""
        if gzip_enabled:
            payload = gzip.compress(payload)
        return self._send_or_spool_payload(
            dataset=dataset,
            payload=payload,
            gzip_enabled=gzip_enabled,
            row_count=0,
            dry_run=True,
        )

    def flush_spool(self, max_files: Optional[int] = None) -> int:
        max_files = int(max_files or self.runtime.spool_flush_max_files)
        flushed = 0
        for item in self.spool.pending():
            if flushed >= max_files:
                break

            payload = item.path.read_bytes()
            try:
                self._send_payload(
                    dataset=item.dataset,
                    payload=payload,
                    gzip_enabled=item.gzip_enabled,
                    dry_run=False,
                    row_count=item.row_count,
                    delivery_key=item.delivery_key or self._delivery_key(item.dataset, payload),
                )
                self.spool.remove(item)
                flushed += 1
                self.logger.info(
                    "dataset=%s phase=spool_flush_ok file=%s rows=%s attempts=%s",
                    item.dataset,
                    item.path.name,
                    item.row_count,
                    item.attempts,
                )
            except RetryableError as exc:
                retry_item = self.spool.mark_attempt(item, error=str(exc))
                self.logger.warning(
                    "dataset=%s phase=spool_flush_retry_later file=%s attempts=%s error=%s",
                    retry_item.dataset,
                    retry_item.path.name,
                    retry_item.attempts,
                    str(exc)[:300],
                )
                break
            except PermanentPayloadError as exc:
                dead_item = self.spool.move_to_dead_letter(item, reason=str(exc))
                self.logger.error(
                    "dataset=%s phase=spool_dead_letter file=%s reason=%s",
                    dead_item.dataset,
                    dead_item.path.name,
                    str(exc)[:300],
                )
                continue
            except PermanentTransportError as exc:
                blocked_item = self.spool.mark_attempt(item, error=str(exc))
                self.logger.error(
                    "dataset=%s phase=spool_blocked file=%s attempts=%s reason=%s",
                    blocked_item.dataset,
                    blocked_item.path.name,
                    blocked_item.attempts,
                    str(exc)[:300],
                )
                break
        return flushed

    def spool_status(self) -> Dict:
        return self.spool.stats()

    def _send_payload(
        self,
        *,
        dataset: str,
        payload: bytes,
        gzip_enabled: bool,
        dry_run: bool,
        row_count: int,
        delivery_key: str,
    ) -> Dict:
        url = self._url(f"/ingest/{dataset}")
        headers = self._headers(include_auth=True, include_content_type=True, idempotency_key=delivery_key)
        if gzip_enabled:
            headers["Content-Encoding"] = "gzip"

        self.logger.info(
            "dataset=%s phase=send_start url=%s rows=%s gzip=%s connect_timeout_s=%s read_timeout_s=%s delivery_key=%s",
            dataset,
            url,
            row_count,
            gzip_enabled,
            self.runtime.effective_connect_timeout_seconds,
            self.runtime.effective_read_timeout_seconds,
            delivery_key[:18],
        )

        def _call():
            try:
                resp = self._request("POST", url, headers=headers, data=payload)
            except (requests.Timeout, requests.ConnectionError) as exc:
                raise RetryableError(f"{exc.__class__.__name__}: {str(exc)}") from exc
            except requests.RequestException as exc:
                raise RetryableError(f"RequestException: {str(exc)}") from exc

            if resp.status_code >= 400:
                self._raise_for_response(resp, dataset=dataset, url=url)
            return resp

        resp = retry_with_backoff(
            _call,
            max_retries=self.runtime.max_retries,
            base_sleep_seconds=self.runtime.retry_backoff_base_seconds,
            max_sleep_seconds=self.runtime.retry_backoff_max_seconds,
            jitter_seconds=self.runtime.retry_jitter_seconds,
            on_retry=lambda i, e: self.logger.warning(
                "dataset=%s phase=retry retry=%s error=%s",
                dataset,
                i,
                str(e)[:300],
            ),
        )

        body = self._body_json(resp)
        self.logger.info(
            "dataset=%s phase=send_done status=%s inserted_or_updated=%s rejected=%s delivery_key=%s",
            dataset,
            resp.status_code,
            body.get("inserted_or_updated"),
            body.get("rejected"),
            delivery_key[:18],
        )
        result = {
            "ok": True,
            "status_code": int(resp.status_code),
            "delivery_key": delivery_key,
            "inserted_or_updated": body.get("inserted_or_updated", 0),
            "rejected": body.get("rejected", 0),
        }
        if dry_run:
            result["body"] = body
        else:
            result.update(body)
        return result

    def _send_or_spool_payload(
        self,
        *,
        dataset: str,
        payload: bytes,
        gzip_enabled: bool,
        row_count: int,
        dry_run: bool,
    ) -> Dict:
        delivery_key = self._delivery_key(dataset, payload)
        try:
            return self._send_payload(
                dataset=dataset,
                payload=payload,
                gzip_enabled=gzip_enabled,
                dry_run=dry_run,
                row_count=row_count,
                delivery_key=delivery_key,
            )
        except RetryableError as exc:
            if dry_run:
                raise

            item = self.spool.enqueue(
                dataset=dataset,
                payload=payload,
                gzip_enabled=gzip_enabled,
                row_count=row_count,
                delivery_key=delivery_key,
                failure_reason=str(exc),
            )
            phase = "spool_duplicate" if item.deduplicated else "spooled"
            log_method = self.logger.warning if item.deduplicated else self.logger.error
            log_method(
                "dataset=%s phase=%s file=%s rows=%s reason=%s suggestion=%s",
                dataset,
                phase,
                item.path.name,
                row_count,
                str(exc)[:300],
                "validar conectividade API, chave X-Ingest-Key e status do proxy /api",
            )
            return {
                "ok": True,
                "spooled": True,
                "inserted_or_updated": 0,
                "rejected": 0,
                "spool_file": item.path.name,
                "delivery_key": delivery_key,
            }

    def send(self, dataset: str, rows: Iterable[Dict], dry_run: bool = False) -> Dict:
        flushed = self.flush_spool()
        if flushed:
            self.logger.info("phase=spool_flush_summary files=%s", flushed)

        row_list = list(rows)
        payload = to_ndjson_bytes(row_list)
        if self.runtime.gzip_enabled:
            payload = gzip.compress(payload)

        return self._send_or_spool_payload(
            dataset=dataset,
            payload=payload,
            gzip_enabled=self.runtime.gzip_enabled,
            row_count=len(row_list),
            dry_run=dry_run,
        )

    def send_file(self, dataset: str, file_path: str | Path, dry_run: bool = False) -> Dict:
        flushed = self.flush_spool()
        if flushed:
            self.logger.info("phase=spool_flush_summary files=%s", flushed)

        path = Path(file_path)
        payload = path.read_bytes()
        gzip_enabled = path.suffix.lower() == ".gz"
        if gzip_enabled:
            try:
                row_count = len([line for line in gzip.decompress(payload).splitlines() if line.strip()])
            except Exception:
                row_count = 0
        else:
            row_count = len([line for line in payload.splitlines() if line.strip()])

        return self._send_or_spool_payload(
            dataset=dataset,
            payload=payload,
            gzip_enabled=gzip_enabled,
            row_count=row_count,
            dry_run=dry_run,
        )
