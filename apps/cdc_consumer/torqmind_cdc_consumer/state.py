"""Consumer state management - offset tracking."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ConsumerState:
    """Tracks consumer offsets and statistics."""

    events_processed: int = 0
    events_errors: int = 0
    last_offsets: dict[str, int] = field(default_factory=dict)

    def record_offset(self, topic: str, partition: int, offset: int) -> None:
        """Record the latest processed offset."""
        key = f"{topic}:{partition}"
        self.last_offsets[key] = max(self.last_offsets.get(key, -1), offset)

    def increment_processed(self) -> None:
        self.events_processed += 1

    def increment_errors(self) -> None:
        self.events_errors += 1
