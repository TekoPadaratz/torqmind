"""Unit tests for company branding (pure logic; no DB required).

Covers the security-critical pieces: magic-number validation (not extension),
safe filenames (no path traversal), the public contract (fallback to default),
the size limit and the permission guard on upload.
"""
from __future__ import annotations

import unittest
from unittest.mock import patch

from app import repos_branding
from app.repos_branding import (
    BrandingError,
    _sniff_image,
    _kind_guard,
    _safe_filename,
    _row_to_public,
)

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 32
JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 32
GIF = b"GIF89a" + b"\x00" * 32
WEBP = b"RIFF\x00\x00\x00\x00WEBP" + b"\x00" * 32
SVG = b"<svg xmlns='http://www.w3.org/2000/svg'></svg>"


class SniffImageTest(unittest.TestCase):
    def test_png(self):
        self.assertEqual(_sniff_image(PNG), ("image/png", "png"))

    def test_jpeg(self):
        self.assertEqual(_sniff_image(JPEG), ("image/jpeg", "jpg"))

    def test_gif(self):
        self.assertEqual(_sniff_image(GIF), ("image/gif", "gif"))

    def test_webp(self):
        self.assertEqual(_sniff_image(WEBP), ("image/webp", "webp"))

    def test_svg_rejected(self):
        with self.assertRaises(BrandingError) as ctx:
            _sniff_image(SVG)
        self.assertEqual(ctx.exception.error, "unsupported_format")

    def test_renamed_executable_rejected(self):
        # A .png by name but ELF/script bytes must be rejected by magic number.
        with self.assertRaises(BrandingError):
            _sniff_image(b"\x7fELF\x02\x01\x01\x00 not a real image at all")

    def test_too_short_rejected(self):
        with self.assertRaises(BrandingError) as ctx:
            _sniff_image(b"\x89PNG")
        self.assertEqual(ctx.exception.error, "invalid_image")


class KindGuardTest(unittest.TestCase):
    def test_valid(self):
        _kind_guard("background")
        _kind_guard("logo")

    def test_invalid(self):
        with self.assertRaises(BrandingError):
            _kind_guard("../etc/passwd")
        with self.assertRaises(BrandingError):
            _kind_guard("favicon")


class SafeFilenameTest(unittest.TestCase):
    def test_filename_is_server_controlled(self):
        name = _safe_filename(1, "background", "abc123def456", "webp")
        self.assertEqual(name, "company_1_background_abc123def456.webp")

    def test_filename_coerces_int_empresa(self):
        # Even if a hostile value reached here, int() coercion removes traversal.
        name = _safe_filename(7, "logo", "deadbeef", "png")
        self.assertNotIn("/", name)
        self.assertNotIn("..", name)


class PublicContractTest(unittest.TestCase):
    def test_no_row_uses_default(self):
        out = _row_to_public(1, None)
        self.assertTrue(out["uses_default"])
        self.assertIsNone(out["background_url"])
        self.assertIsNone(out["logo_url"])

    def test_versioned_urls(self):
        row = {
            "background_image_path": "company_1_background_v1.webp",
            "background_version": "v1hash",
            "logo_image_path": "company_1_logo_v2.png",
            "logo_version": "v2hash",
        }
        out = _row_to_public(1, row)
        self.assertFalse(out["uses_default"])
        self.assertEqual(out["background_url"], "/api/branding/1/background?v=v1hash")
        self.assertEqual(out["logo_url"], "/api/branding/1/logo?v=v2hash")

    def test_partial_logo_only(self):
        row = {
            "logo_image_path": "company_1_logo_v2.png",
            "logo_version": "v2",
        }
        out = _row_to_public(1, row)
        self.assertFalse(out["uses_default"])
        self.assertIsNone(out["background_url"])
        self.assertEqual(out["logo_url"], "/api/branding/1/logo?v=v2")


class SaveImageGuardsTest(unittest.TestCase):
    def _claims(self):
        return {"sub": "00000000-0000-0000-0000-000000000001", "user_role": "platform_master"}

    def test_permission_denied_before_touching_disk(self):
        from app import repos_platform

        err = repos_platform.AuthError(403, "platform_forbidden", "nope")
        with patch.object(repos_branding.repos_platform, "_assert_company_mutable", side_effect=err):
            with self.assertRaises(repos_platform.AuthError):
                repos_branding.save_image(self._claims(), 1, "background", PNG)

    def test_empty_file_rejected(self):
        with patch.object(repos_branding.repos_platform, "_assert_company_mutable", return_value={}):
            with self.assertRaises(BrandingError) as ctx:
                repos_branding.save_image(self._claims(), 1, "background", b"")
            self.assertEqual(ctx.exception.error, "empty_file")

    def test_oversize_rejected(self):
        big = b"\x89PNG\r\n\x1a\n" + b"\x00" * (repos_branding.MAX_FILE_BYTES + 10)
        with patch.object(repos_branding.repos_platform, "_assert_company_mutable", return_value={}):
            with self.assertRaises(BrandingError) as ctx:
                repos_branding.save_image(self._claims(), 1, "background", big)
            self.assertEqual(ctx.exception.error, "file_too_large")

    def test_bad_format_rejected_after_permission(self):
        with patch.object(repos_branding.repos_platform, "_assert_company_mutable", return_value={}):
            with self.assertRaises(BrandingError) as ctx:
                repos_branding.save_image(self._claims(), 1, "logo", SVG)
            self.assertEqual(ctx.exception.error, "unsupported_format")


if __name__ == "__main__":
    unittest.main()
