from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, List, Optional


@dataclass
class ExtractBatch:
    rows: List[dict]
    max_watermark: Optional[str]
    extracted_at: datetime


class BaseExtractor:
    def check_connection(self) -> None:
        raise NotImplementedError

    def iter_batches(
        self,
        dataset: str,
        watermark: Optional[str],
        batch_size: int,
        fetch_size: int,
        dt_from: Optional[datetime] = None,
        dt_to: Optional[datetime] = None,
    ) -> Iterator[ExtractBatch]:
        raise NotImplementedError
