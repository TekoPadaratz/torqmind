from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Optional

from agent.utils.timezone import business_datetime_iso, ensure_business_datetime


@dataclass
class WatermarkRecord:
    dataset: str
    scope: str
    watermark: Optional[str]
    updated_at: str


@dataclass(frozen=True)
class IncrementalCursor:
    last_watermark: Optional[str]
    last_pk_tuple: Optional[list[Any]] = None


class WatermarkStore:
    def __init__(self, root_dir: str, tenant_key: str) -> None:
        self.root = Path(root_dir)
        self.tenant_dir = self.root / tenant_key
        self.tenant_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, dataset: str) -> Path:
        return self.tenant_dir / f"{dataset.lower()}.json"

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    def _load(self, dataset: str) -> Dict[str, Any]:
        path = self._path(dataset)
        if not path.exists():
            return {
                "dataset": dataset.lower(),
                "watermarks": {},
                "cursors": {},
                "bootstrap": {},
                "scope_state": {},
            }
        data = json.loads(path.read_text(encoding="utf-8"))
        data.setdefault("dataset", dataset.lower())
        data.setdefault("watermarks", {})
        data.setdefault("cursors", {})
        data.setdefault("bootstrap", {})
        data.setdefault("scope_state", {})
        return data

    def _save(self, dataset: str, data: Dict[str, Any]) -> None:
        path = self._path(dataset)
        data.setdefault("dataset", dataset.lower())
        data.setdefault("watermarks", {})
        data.setdefault("cursors", {})
        data.setdefault("bootstrap", {})
        data.setdefault("scope_state", {})
        data["updated_at"] = self._utc_now_iso()

        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    def get(self, dataset: str, scope: str = "default") -> Optional[str]:
        return self.get_cursor(dataset, scope=scope).last_watermark

    def get_cursor(self, dataset: str, scope: str = "default") -> IncrementalCursor:
        data = self._load(dataset)
        cursors = data.get("cursors", {})
        stored_cursor = cursors.get(scope)
        if isinstance(stored_cursor, dict):
            return IncrementalCursor(
                last_watermark=self.normalize_watermark(stored_cursor.get("last_watermark")),
                last_pk_tuple=self._deserialize_pk_tuple(stored_cursor.get("last_pk_tuple")),
            )

        watermarks = data.get("watermarks", {})
        if scope in watermarks:
            return IncrementalCursor(last_watermark=self.normalize_watermark(watermarks[scope]))
        if "watermark" in data:
            # compatibility with initial state shape
            return IncrementalCursor(last_watermark=self.normalize_watermark(data.get("watermark")))
        return IncrementalCursor(last_watermark=None, last_pk_tuple=None)

    def set(self, dataset: str, watermark: Optional[str], scope: str = "default") -> None:
        self.set_cursor(dataset, IncrementalCursor(last_watermark=watermark, last_pk_tuple=None), scope=scope)

    def set_cursor(self, dataset: str, cursor: IncrementalCursor, scope: str = "default") -> None:
        data = self._load(dataset)
        normalized = self.normalize_watermark(cursor.last_watermark)
        data["watermarks"][scope] = normalized
        data.setdefault("cursors", {})
        data["cursors"][scope] = {
            "last_watermark": normalized,
            "last_pk_tuple": self._serialize_pk_tuple(cursor.last_pk_tuple),
        }
        self._save(dataset, data)

    def is_bootstrap_complete(self, dataset: str, scope: str = "default") -> bool:
        data = self._load(dataset)
        return bool((data.get("bootstrap", {}).get(scope) or {}).get("completed", False))

    def mark_bootstrap_complete(self, dataset: str, scope: str = "default") -> None:
        data = self._load(dataset)
        bootstrap = data.setdefault("bootstrap", {})
        bootstrap[scope] = {
            "completed": True,
            "completed_at": self._utc_now_iso(),
        }
        self._save(dataset, data)

    def clear_bootstrap(self, dataset: str, scope: str = "default") -> None:
        data = self._load(dataset)
        bootstrap = data.setdefault("bootstrap", {})
        if scope in bootstrap:
            bootstrap.pop(scope, None)
            self._save(dataset, data)

    def get_scope_value(self, dataset: str, key: str, scope: str = "default", default: Any = None) -> Any:
        data = self._load(dataset)
        scope_state = data.setdefault("scope_state", {})
        scoped = scope_state.get(scope, {})
        if not isinstance(scoped, dict):
            return default
        return scoped.get(key, default)

    def set_scope_value(self, dataset: str, key: str, value: Any, scope: str = "default") -> None:
        data = self._load(dataset)
        scope_state = data.setdefault("scope_state", {})
        scoped = scope_state.setdefault(scope, {})
        if not isinstance(scoped, dict):
            scoped = {}
            scope_state[scope] = scoped
        scoped[key] = value
        self._save(dataset, data)

    def clear_scope_value(self, dataset: str, key: str, scope: str = "default") -> None:
        data = self._load(dataset)
        scope_state = data.setdefault("scope_state", {})
        scoped = scope_state.get(scope)
        if not isinstance(scoped, dict) or key not in scoped:
            return
        scoped.pop(key, None)
        if not scoped:
            scope_state.pop(scope, None)
        self._save(dataset, data)

    @staticmethod
    def migrate_legacy_state(
        legacy_state_path: str,
        target_store: "WatermarkStore",
        scope: str = "default",
        overwrite_existing: bool = False,
    ) -> int:
        legacy_path = Path(legacy_state_path)
        if not legacy_path.exists():
            return 0

        data = json.loads(legacy_path.read_text(encoding="utf-8"))
        migrated = 0
        for dataset, value in data.items():
            if not value:
                continue
            ds = dataset.lower()
            if not overwrite_existing and target_store.get(ds, scope=scope):
                continue
            target_store.set(ds, target_store.normalize_watermark(str(value)), scope=scope)
            migrated += 1
        return migrated

    @staticmethod
    def _serialize_pk_scalar(value: Any) -> Any:
        if isinstance(value, datetime):
            return business_datetime_iso(ensure_business_datetime(value), timespec="microseconds")
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        return str(value)

    @classmethod
    def _serialize_pk_tuple(cls, values: Optional[list[Any]]) -> Optional[list[Any]]:
        if values is None or len(values) == 0:
            return None
        return [cls._serialize_pk_scalar(value) for value in values]

    @staticmethod
    def _deserialize_pk_tuple(values: Any) -> Optional[list[Any]]:
        if not isinstance(values, list) or not values:
            return None
        return list(values)

    @staticmethod
    def parse_watermark_dt(value: Optional[str]) -> Optional[datetime]:
        if value is None:
            return None
        raw = str(value).strip()
        if not raw:
            return None

        # Preferred format: ISO 8601
        try:
            return ensure_business_datetime(datetime.fromisoformat(raw))
        except ValueError:
            pass

        # Common legacy format seen in old state
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
            try:
                return ensure_business_datetime(datetime.strptime(raw, fmt))
            except ValueError:
                continue

        return None

    @staticmethod
    def normalize_watermark(value: Optional[str]) -> Optional[str]:
        dt = WatermarkStore.parse_watermark_dt(value)
        if dt is None:
            return None if value is None else str(value).strip()
        return business_datetime_iso(dt, timespec="microseconds")
