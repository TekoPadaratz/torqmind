from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import uuid


@dataclass
class SpoolItem:
    path: Path
    dataset: str
    gzip_enabled: bool
    created_at: str


class SpoolQueue:
    def __init__(self, root_dir: str) -> None:
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)

    def enqueue(self, dataset: str, payload: bytes, gzip_enabled: bool) -> SpoolItem:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        suffix = ".ndjson.gz" if gzip_enabled else ".ndjson"
        safe_ds = dataset.strip().lower().replace("/", "_")
        name = f"{ts}_{uuid.uuid4().hex}_{safe_ds}{suffix}"
        path = self.root / name
        path.write_bytes(payload)
        return self._to_item(path)

    def pending(self):
        for path in sorted(self.root.glob("*.ndjson*")):
            if path.is_file():
                yield self._to_item(path)

    def remove(self, item: SpoolItem) -> None:
        if item.path.exists():
            item.path.unlink()

    @staticmethod
    def _to_item(path: Path) -> SpoolItem:
        name = path.name
        gzip_enabled = name.endswith(".ndjson.gz")
        parts = name.split("_", 2)
        created_at = parts[0] if parts else ""
        dataset = "unknown"
        if len(parts) >= 3:
            ds = parts[2]
            ds = ds[:-10] if ds.endswith(".ndjson.gz") else ds[:-7] if ds.endswith(".ndjson") else ds
            dataset = ds
        return SpoolItem(path=path, dataset=dataset, gzip_enabled=gzip_enabled, created_at=created_at)
