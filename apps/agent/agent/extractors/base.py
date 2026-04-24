from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator, List, Optional


@dataclass
class ExtractBatch:
    rows: List[dict]
    max_watermark: Optional[str]
    extracted_at: datetime
    last_pk_tuple: Optional[list[Any]] = None


class BaseExtractor:
    def check_connection(self) -> None:
        raise NotImplementedError

    def preflight_dataset(self, dataset: str) -> None:
        return None

    def iter_batches(
        self,
        dataset: str,
        watermark: Optional[str],
        batch_size: int,
        fetch_size: int,
        cursor_pk_tuple: Optional[list[Any]] = None,
        dt_from: Optional[datetime] = None,
        dt_to: Optional[datetime] = None,
    ) -> Iterator[ExtractBatch]:
        raise NotImplementedError
