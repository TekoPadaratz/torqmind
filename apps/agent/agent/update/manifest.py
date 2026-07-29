"""Agent self-update — manifest parse, semver compare, SHA256 verify."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


_SEMVER_RE = re.compile(
    r"^v?(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"
    r"(?:[-+](?P<label>[0-9A-Za-z.-]+))?$"
)


@dataclass(frozen=True)
class ReleaseManifest:
    version: str
    sha256: str
    size: int
    url: str
    released_at: Optional[str] = None
    min_version: Optional[str] = None
    mandatory: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ReleaseManifest":
        version = str(data.get("version") or "").strip()
        sha256 = str(data.get("sha256") or "").strip().lower()
        url = str(data.get("url") or "").strip()
        if not version or not sha256 or not url:
            raise ValueError("manifest requires version, sha256, url")
        try:
            size = int(data.get("size") or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError("manifest size must be int") from exc
        return cls(
            version=version,
            sha256=sha256,
            size=size,
            url=url,
            released_at=(str(data["released_at"]) if data.get("released_at") else None),
            min_version=(str(data["min_version"]).strip() if data.get("min_version") else None),
            mandatory=bool(data.get("mandatory", False)),
        )


def parse_semver(version: str) -> Tuple[int, int, int, str]:
    text = str(version or "").strip()
    match = _SEMVER_RE.match(text)
    if not match:
        # Fallback: treat non-semver build tags as (0,0,0, full) so numbered 2.0.0 wins.
        return (0, 0, 0, text)
    return (
        int(match.group("major")),
        int(match.group("minor")),
        int(match.group("patch")),
        match.group("label") or "",
    )


def _is_legacy_calendar_version(parts: Tuple[int, int, int, str]) -> bool:
    """Old agent tags used YYYY.MM.DD (+label). Product semver stays below year majors."""
    return parts[0] >= 2000


def is_newer_version(remote: str, local: str) -> bool:
    """True if remote version should replace local (product semver beats calendar tags)."""
    r = parse_semver(remote)
    l = parse_semver(local)
    # 1.x calendar builds (e.g. 2026.07.28+turnos) must upgrade to 2.0.0.
    if _is_legacy_calendar_version(l) and not _is_legacy_calendar_version(r):
        return True
    if _is_legacy_calendar_version(r) and not _is_legacy_calendar_version(l):
        return False
    if r[:3] != l[:3]:
        return r[:3] > l[:3]
    # Same x.y.z — prefer remote if local has a pre-release label and remote is clean,
    # or if labels differ and remote sorts higher.
    if not r[3] and l[3]:
        return True
    if r[3] and not l[3]:
        return False
    return r[3] > l[3]


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def verify_sha256(path: Path, expected: str) -> bool:
    got = sha256_file(path)
    return got.lower() == str(expected or "").strip().lower()


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
