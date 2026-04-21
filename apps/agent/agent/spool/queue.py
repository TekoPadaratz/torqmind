from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import uuid


@dataclass
class SpoolItem:
    path: Path
    meta_path: Path
    dataset: str
    gzip_enabled: bool
    created_at: str
    row_count: int = 0
    payload_sha256: str = ""
    delivery_key: str = ""
    attempts: int = 0
    last_error: str = ""
    deduplicated: bool = False


class SpoolQueue:
    def __init__(self, root_dir: str) -> None:
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.dead_letter_dir = self.root / "dead-letter"
        self.dead_letter_dir.mkdir(parents=True, exist_ok=True)

    def enqueue(
        self,
        dataset: str,
        payload: bytes,
        gzip_enabled: bool,
        *,
        row_count: int = 0,
        delivery_key: str = "",
        failure_reason: str = "",
    ) -> SpoolItem:
        payload_sha256 = hashlib.sha256(payload).hexdigest()
        duplicate = self.find_duplicate(dataset=dataset, payload_sha256=payload_sha256)
        if duplicate is not None:
            if failure_reason:
                self._update_meta(duplicate, last_error=failure_reason)
            duplicate.deduplicated = True
            return duplicate

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        suffix = ".ndjson.gz" if gzip_enabled else ".ndjson"
        safe_ds = dataset.strip().lower().replace("/", "_")
        name = f"{ts}_{uuid.uuid4().hex}_{safe_ds}{suffix}"
        path = self.root / name
        path.write_bytes(payload)
        item = self._to_item(path)
        self._write_meta(
            item,
            {
                "dataset": safe_ds,
                "gzip_enabled": gzip_enabled,
                "created_at": ts,
                "row_count": int(row_count),
                "payload_sha256": payload_sha256,
                "delivery_key": delivery_key,
                "attempts": 0,
                "last_error": failure_reason or "",
            },
        )
        return self._to_item(path)

    def pending(self):
        for path in sorted(self.root.glob("*.ndjson*")):
            if path.is_file() and not path.name.endswith(".meta.json"):
                yield self._to_item(path)

    def remove(self, item: SpoolItem) -> None:
        if item.path.exists():
            item.path.unlink()
        if item.meta_path.exists():
            item.meta_path.unlink()

    def move_to_dead_letter(self, item: SpoolItem, reason: str) -> SpoolItem:
        payload_target = self.dead_letter_dir / item.path.name
        meta_target = self.dead_letter_dir / item.meta_path.name

        payload_target.parent.mkdir(parents=True, exist_ok=True)
        item.path.replace(payload_target)

        meta = self._load_meta(item.meta_path)
        meta["dead_lettered_at"] = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        meta["last_error"] = str(reason or "")[:500]
        item.meta_path.parent.mkdir(parents=True, exist_ok=True)
        meta_target.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        if item.meta_path.exists():
            item.meta_path.unlink()

        return self._to_item(payload_target)

    def mark_attempt(self, item: SpoolItem, error: str = "") -> SpoolItem:
        attempts = int(item.attempts or 0) + 1
        updated = self._update_meta(
            item,
            attempts=attempts,
            last_error=str(error or item.last_error or "")[:500],
            last_attempt_at=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"),
        )
        return updated

    def stats(self) -> dict:
        pending_items = list(self.pending())
        dead_letter = list(self.dead_letter_dir.glob("*.ndjson*"))
        dead_letter_files = [path for path in dead_letter if path.is_file() and not path.name.endswith(".meta.json")]
        return {
            "pending_files": len(pending_items),
            "pending_rows": sum(int(item.row_count or 0) for item in pending_items),
            "dead_letter_files": len(dead_letter_files),
        }

    def find_duplicate(self, dataset: str, payload_sha256: str) -> SpoolItem | None:
        safe_ds = dataset.strip().lower().replace("/", "_")
        for item in self.pending():
            if item.dataset == safe_ds and item.payload_sha256 == payload_sha256:
                return item
        return None

    @staticmethod
    def _meta_path(path: Path) -> Path:
        return Path(str(path) + ".meta.json")

    def _load_meta(self, meta_path: Path) -> dict:
        if not meta_path.exists():
            return {}
        try:
            return json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _write_meta(self, item: SpoolItem, meta: dict) -> None:
        item.meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    def _update_meta(self, item: SpoolItem, **updates) -> SpoolItem:
        meta = self._load_meta(item.meta_path)
        meta.update({k: v for k, v in updates.items() if v is not None})
        self._write_meta(item, meta)
        return self._to_item(item.path)

    def _to_item(self, path: Path) -> SpoolItem:
        name = path.name
        gzip_enabled = name.endswith(".ndjson.gz")
        meta_path = self._meta_path(path)
        meta = self._load_meta(meta_path)
        parts = name.split("_", 2)
        created_at = str(meta.get("created_at") or (parts[0] if parts else ""))
        dataset = str(meta.get("dataset") or "unknown")
        if dataset == "unknown" and len(parts) >= 3:
            ds = parts[2]
            ds = ds[:-10] if ds.endswith(".ndjson.gz") else ds[:-7] if ds.endswith(".ndjson") else ds
            dataset = ds
        return SpoolItem(
            path=path,
            meta_path=meta_path,
            dataset=dataset,
            gzip_enabled=bool(meta.get("gzip_enabled", gzip_enabled)),
            created_at=created_at,
            row_count=int(meta.get("row_count", 0) or 0),
            payload_sha256=str(meta.get("payload_sha256") or ""),
            delivery_key=str(meta.get("delivery_key") or ""),
            attempts=int(meta.get("attempts", 0) or 0),
            last_error=str(meta.get("last_error") or ""),
        )
