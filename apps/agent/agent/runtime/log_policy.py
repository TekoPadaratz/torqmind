"""Quiet logging + one JSON summary line per cycle (Agent 2.0)."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional


# Batch/detail noise stays at DEBUG; cycle lifecycle at INFO.
QUIET_DEFAULT_LEVEL = logging.INFO


def resolve_log_level(raw: Optional[str], *, quiet_default: bool = True) -> int:
    text = str(raw or "").strip().upper()
    if not text:
        # 2.0: INFO for cycle events; callers log batch detail at DEBUG.
        return QUIET_DEFAULT_LEVEL if quiet_default else logging.INFO
    return getattr(logging, text, logging.INFO)


def build_logger(
    name: str = "torqmind-agent",
    level: int = logging.INFO,
    *,
    log_file: Optional[str] = None,
    max_bytes: int = 5_000_000,
    backup_count: int = 5,
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    stream.setLevel(level)
    logger.addHandler(stream)

    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            path,
            maxBytes=max(100_000, int(max_bytes)),
            backupCount=max(1, int(backup_count)),
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger


def append_summary_line(summary_path: str, line: str) -> None:
    if not str(summary_path or "").strip():
        return
    path = Path(summary_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line.rstrip() + "\n")


def write_cycle_summary(
    summary_path: str,
    payload: Dict[str, Any],
    *,
    max_lines: int = 5000,
) -> None:
    """Append one JSON object per cycle; trim file if it grows too large."""
    if not str(summary_path or "").strip():
        return
    body = dict(payload)
    body.setdefault("ts", datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
    line = json.dumps(body, ensure_ascii=False, separators=(",", ":"), default=str)
    path = Path(summary_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        if len(lines) > max_lines:
            path.write_text("\n".join(lines[-max_lines:]) + "\n", encoding="utf-8")
    except OSError:
        pass
