from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class WatermarkRecord:
    dataset: str
    scope: str
    watermark: Optional[str]
    updated_at: str


class WatermarkStore:
    def __init__(self, root_dir: str, tenant_key: str) -> None:
        self.root = Path(root_dir)
        self.tenant_dir = self.root / tenant_key
        self.tenant_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, dataset: str) -> Path:
        return self.tenant_dir / f"{dataset.lower()}.json"

    def get(self, dataset: str, scope: str = "default") -> Optional[str]:
        path = self._path(dataset)
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        watermarks = data.get("watermarks", {})
        if scope in watermarks:
            return watermarks[scope]
        if "watermark" in data:
            # compatibility with initial state shape
            return data.get("watermark")
        return None

    def set(self, dataset: str, watermark: Optional[str], scope: str = "default") -> None:
        path = self._path(dataset)
        now_iso = datetime.now(timezone.utc).isoformat()

        data: Dict[str, Any]
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
        else:
            data = {"dataset": dataset.lower(), "watermarks": {}}

        data.setdefault("dataset", dataset.lower())
        data.setdefault("watermarks", {})
        data["watermarks"][scope] = watermark
        data["updated_at"] = now_iso

        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    @staticmethod
    def migrate_legacy_state(legacy_state_path: str, target_store: "WatermarkStore", scope: str = "default") -> int:
        legacy_path = Path(legacy_state_path)
        if not legacy_path.exists():
            return 0

        data = json.loads(legacy_path.read_text(encoding="utf-8"))
        migrated = 0
        for dataset, value in data.items():
            if not value:
                continue
            ds = dataset.lower()
            target_store.set(ds, str(value), scope=scope)
            migrated += 1
        return migrated
