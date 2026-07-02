"""Company branding (background + logo) — storage + metadata.

Files live on a persistent volume (``BRANDING_STORAGE_DIR``, default
``/app/var/branding``) so they survive deploys; PostgreSQL keeps only the
metadata (path, mime, size, version). A company without a row uses the default
TorqMind identity.

Security:
- Upload/delete require platform operations permission AND visibility of the
  company (reused from ``repos_platform``).
- Uploaded bytes are validated by magic number (not just the declared MIME or
  the file extension); only web-safe raster formats are accepted. No SVG.
- Filenames are derived from ``id_empresa`` + kind + version hash; user input
  never reaches the filesystem path (no path traversal).
- The public GET (serving the image to CSS/`<img>`) is intentionally
  unauthenticated because branding assets are non-sensitive and bearer auth
  cannot ride on a CSS ``url()``; the versioned URL provides cache-busting.
"""
from __future__ import annotations

import hashlib
import os
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from app.db import get_conn
from app import repos_platform

# --------------------------------------------------------------------------- #
# Storage configuration
# --------------------------------------------------------------------------- #
STORAGE_DIR = Path(os.environ.get("BRANDING_STORAGE_DIR", "/app/var/branding"))
MAX_FILE_BYTES = int(os.environ.get("BRANDING_MAX_FILE_BYTES", str(6 * 1024 * 1024)))  # 6 MB
VALID_KINDS = ("background", "logo")

# Magic-number signatures -> canonical (mime, extension). Only web-safe rasters.
_SIGNATURES: tuple[tuple[bytes, str, str], ...] = (
    (b"\x89PNG\r\n\x1a\n", "image/png", "png"),
    (b"\xff\xd8\xff", "image/jpeg", "jpg"),
    (b"GIF87a", "image/gif", "gif"),
    (b"GIF89a", "image/gif", "gif"),
)


class BrandingError(Exception):
    def __init__(self, status_code: int, error: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error = error
        self.message = message

    def as_detail(self) -> dict[str, Any]:
        return {"error": self.error, "message": self.message}


def _sniff_image(content: bytes) -> tuple[str, str]:
    """Return (mime, ext) from the magic number, or raise BrandingError.

    WebP needs a two-part check: ``RIFF`` .... ``WEBP``.
    """
    if len(content) < 12:
        raise BrandingError(422, "invalid_image", "Arquivo de imagem inválido ou vazio.")
    if content[0:4] == b"RIFF" and content[8:12] == b"WEBP":
        return "image/webp", "webp"
    for sig, mime, ext in _SIGNATURES:
        if content.startswith(sig):
            return mime, ext
    raise BrandingError(
        422,
        "unsupported_format",
        "Formato não suportado. Envie PNG, JPG, WebP ou GIF.",
    )


def _kind_guard(kind: str) -> None:
    if kind not in VALID_KINDS:
        raise BrandingError(422, "invalid_kind", "Tipo de imagem inválido.")


def _safe_filename(id_empresa: int, kind: str, version: str, ext: str) -> str:
    # All inputs are server-controlled integers / whitelisted strings.
    return f"company_{int(id_empresa)}_{kind}_{version}.{ext}"


def _row_to_public(id_empresa: int, row: Optional[dict[str, Any]]) -> dict[str, Any]:
    """Public branding contract for the session/bootstrap.

    Always returns ``uses_default`` so the frontend knows whether to fall back to
    the TorqMind identity. URLs are versioned for cache-busting.
    """
    bg_path = (row or {}).get("background_image_path")
    logo_path = (row or {}).get("logo_image_path")
    bg_ver = (row or {}).get("background_version")
    logo_ver = (row or {}).get("logo_version")
    background_url = (
        f"/api/branding/{int(id_empresa)}/background?v={bg_ver}" if bg_path and bg_ver else None
    )
    logo_url = (
        f"/api/branding/{int(id_empresa)}/logo?v={logo_ver}" if logo_path and logo_ver else None
    )
    return {
        "id_empresa": int(id_empresa),
        "background_url": background_url,
        "logo_url": logo_url,
        "background_version": bg_ver,
        "logo_version": logo_ver,
        "uses_default": not (background_url or logo_url),
    }


def get_branding_public(id_empresa: Optional[int]) -> dict[str, Any]:
    """Lightweight branding contract for a company (no permission check).

    Used by the session builder. Never raises for a missing row — returns the
    default-identity contract instead.
    """
    if not id_empresa:
        return _row_to_public(0, None)
    try:
        with get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM app.company_branding WHERE id_empresa = %s",
                (int(id_empresa),),
            ).fetchone()
    except Exception:
        return _row_to_public(int(id_empresa), None)
    return _row_to_public(int(id_empresa), dict(row) if row else None)


def get_branding_detail(claims: dict[str, Any], id_empresa: int) -> dict[str, Any]:
    """Full branding metadata for the Platform editor (requires visibility)."""
    repos_platform._assert_company_visible(claims, id_empresa)
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM app.company_branding WHERE id_empresa = %s",
            (int(id_empresa),),
        ).fetchone()
    public = _row_to_public(int(id_empresa), dict(row) if row else None)
    if row:
        public["background_file_size"] = row.get("background_file_size")
        public["background_mime_type"] = row.get("background_mime_type")
        public["logo_file_size"] = row.get("logo_file_size")
        public["logo_mime_type"] = row.get("logo_mime_type")
        public["updated_at"] = row.get("updated_at")
    return public


def _read_file_bytes(id_empresa: int, kind: str) -> tuple[bytes, str]:
    """Return (content, mime) for serving. Raises BrandingError(404) if missing."""
    _kind_guard(kind)
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM app.company_branding WHERE id_empresa = %s",
            (int(id_empresa),),
        ).fetchone()
    path_key = f"{kind}_image_path"
    mime_key = f"{kind}_mime_type"
    rel = (dict(row).get(path_key) if row else None) or None
    if not rel:
        raise BrandingError(404, "branding_not_found", "Imagem não encontrada.")
    # rel is server-generated; resolve under STORAGE_DIR and confirm containment.
    full = (STORAGE_DIR / rel).resolve()
    if not str(full).startswith(str(STORAGE_DIR.resolve())) or not full.is_file():
        raise BrandingError(404, "branding_not_found", "Imagem não encontrada.")
    return full.read_bytes(), (dict(row).get(mime_key) or "application/octet-stream")


def serve_image(id_empresa: int, kind: str) -> tuple[bytes, str]:
    """Public serving entrypoint (no auth — branding is non-sensitive)."""
    return _read_file_bytes(id_empresa, kind)


def save_image(claims: dict[str, Any], id_empresa: int, kind: str, content: bytes) -> dict[str, Any]:
    """Validate, store on disk and upsert metadata. Requires mutate permission."""
    _kind_guard(kind)
    repos_platform._assert_company_mutable(claims, id_empresa)

    if not content:
        raise BrandingError(422, "empty_file", "Arquivo vazio.")
    if len(content) > MAX_FILE_BYTES:
        mb = MAX_FILE_BYTES // (1024 * 1024)
        raise BrandingError(413, "file_too_large", f"Arquivo acima do limite de {mb} MB.")

    mime, ext = _sniff_image(content)
    version = hashlib.sha256(content).hexdigest()[:12]
    filename = _safe_filename(id_empresa, kind, version, ext)

    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    dest = STORAGE_DIR / filename
    # Atomic write (temp + replace) to avoid serving a half-written file.
    tmp = STORAGE_DIR / f".{filename}.{secrets.token_hex(4)}.tmp"
    tmp.write_bytes(content)
    os.replace(tmp, dest)

    updated_by = str(claims.get("sub") or "") or None
    now = datetime.now(timezone.utc)
    path_col = f"{kind}_image_path"
    mime_col = f"{kind}_mime_type"
    size_col = f"{kind}_file_size"
    ver_col = f"{kind}_version"

    with get_conn() as conn:
        prev = conn.execute(
            f"SELECT {path_col} AS p FROM app.company_branding WHERE id_empresa = %s",
            (int(id_empresa),),
        ).fetchone()
        conn.execute(
            f"""
            INSERT INTO app.company_branding
                (id_empresa, {path_col}, {mime_col}, {size_col}, {ver_col}, updated_by, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_empresa) DO UPDATE SET
                {path_col} = EXCLUDED.{path_col},
                {mime_col} = EXCLUDED.{mime_col},
                {size_col} = EXCLUDED.{size_col},
                {ver_col}  = EXCLUDED.{ver_col},
                updated_by = EXCLUDED.updated_by,
                updated_at = EXCLUDED.updated_at
            """,
            (int(id_empresa), filename, mime, len(content), version, updated_by, now, now),
        )
        conn.commit()
        old = (dict(prev).get("p") if prev else None)

    # Best-effort cleanup of the superseded file (never fail the request on this).
    if old and old != filename:
        try:
            (STORAGE_DIR / old).unlink(missing_ok=True)
        except Exception:
            pass

    return get_branding_detail(claims, id_empresa)


def delete_image(claims: dict[str, Any], id_empresa: int, kind: str) -> dict[str, Any]:
    """Remove an image and clear its metadata (restores default identity)."""
    _kind_guard(kind)
    repos_platform._assert_company_mutable(claims, id_empresa)

    path_col = f"{kind}_image_path"
    mime_col = f"{kind}_mime_type"
    size_col = f"{kind}_file_size"
    ver_col = f"{kind}_version"
    now = datetime.now(timezone.utc)

    with get_conn() as conn:
        row = conn.execute(
            f"SELECT {path_col} AS p FROM app.company_branding WHERE id_empresa = %s",
            (int(id_empresa),),
        ).fetchone()
        old = (dict(row).get("p") if row else None)
        if row:
            conn.execute(
                f"""
                UPDATE app.company_branding SET
                    {path_col} = NULL, {mime_col} = NULL, {size_col} = NULL, {ver_col} = NULL,
                    updated_by = %s, updated_at = %s
                WHERE id_empresa = %s
                """,
                (str(claims.get("sub") or "") or None, now, int(id_empresa)),
            )
            conn.commit()

    if old:
        try:
            (STORAGE_DIR / old).unlink(missing_ok=True)
        except Exception:
            pass

    return get_branding_detail(claims, id_empresa)
