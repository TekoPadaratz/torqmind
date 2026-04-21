from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict
import base64


class SecretStoreError(RuntimeError):
    pass


CRYPTPROTECT_LOCAL_MACHINE = 0x4


def _secure_zero(buffer) -> None:
    try:
        import ctypes

        ctypes.memset(buffer, 0, len(buffer))
    except Exception:
        return


def _protect_data(plaintext: bytes) -> bytes:
    if os.name != "nt":
        raise SecretStoreError("DPAPI is only available on Windows.")

    import ctypes
    from ctypes import POINTER, Structure, byref, c_char, c_void_p, c_wchar_p, wintypes

    class DATA_BLOB(Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", POINTER(c_char))]

    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32

    in_buffer = ctypes.create_string_buffer(plaintext, len(plaintext))
    in_blob = DATA_BLOB(len(plaintext), ctypes.cast(in_buffer, POINTER(c_char)))
    out_blob = DATA_BLOB()

    try:
        ok = crypt32.CryptProtectData(
            byref(in_blob),
            c_wchar_p("TorqMind Agent Secrets"),
            c_void_p(),
            c_void_p(),
            c_void_p(),
            CRYPTPROTECT_LOCAL_MACHINE,
            byref(out_blob),
        )
        if not ok:
            raise SecretStoreError("Unable to encrypt payload with Windows DPAPI.")
        return ctypes.string_at(out_blob.pbData, out_blob.cbData)
    finally:
        _secure_zero(in_buffer)
        if out_blob.pbData:
            kernel32.LocalFree(out_blob.pbData)


def _unprotect_data(ciphertext: bytes) -> bytes:
    if os.name != "nt":
        raise SecretStoreError("DPAPI is only available on Windows.")

    import ctypes
    from ctypes import POINTER, Structure, byref, c_char, c_void_p, c_wchar_p, wintypes

    class DATA_BLOB(Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", POINTER(c_char))]

    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32

    in_buffer = ctypes.create_string_buffer(ciphertext, len(ciphertext))
    in_blob = DATA_BLOB(len(ciphertext), ctypes.cast(in_buffer, POINTER(c_char)))
    out_blob = DATA_BLOB()
    description = c_wchar_p()

    try:
        ok = crypt32.CryptUnprotectData(
            byref(in_blob),
            byref(description),
            c_void_p(),
            c_void_p(),
            c_void_p(),
            0,
            byref(out_blob),
        )
        if not ok:
            raise SecretStoreError("Unable to decrypt encrypted payload with Windows DPAPI.")
        return ctypes.string_at(out_blob.pbData, out_blob.cbData)
    finally:
        _secure_zero(in_buffer)
        if out_blob.pbData:
            kernel32.LocalFree(out_blob.pbData)


def encrypt_secret_dpapi(plaintext: str) -> bytes:
    raw = bytearray(str(plaintext).encode("utf-8"))
    try:
        return _protect_data(bytes(raw))
    finally:
        _secure_zero(raw)


def decrypt_secret_dpapi(ciphertext: bytes | str) -> str:
    if isinstance(ciphertext, str):
        try:
            decoded = base64.b64decode(ciphertext.encode("ascii"), validate=True)
        except Exception as exc:  # noqa: PERF203
            raise SecretStoreError("Encrypted config payload is invalid.") from exc
        raw = bytearray(decoded)
        try:
            plaintext = _unprotect_data(bytes(raw))
            return plaintext.decode("utf-8")
        finally:
            _secure_zero(raw)

    raw = bytearray(ciphertext)
    try:
        try:
            plaintext = _unprotect_data(bytes(raw))
        except SecretStoreError as original_exc:
            try:
                legacy_bytes = base64.b64decode(bytes(raw), validate=True)
            except Exception:
                raise original_exc
            legacy = bytearray(legacy_bytes)
            try:
                plaintext = _unprotect_data(bytes(legacy))
            finally:
                _secure_zero(legacy)
        return plaintext.decode("utf-8")
    finally:
        _secure_zero(raw)


def save_encrypted_json_file(path: str | Path, payload: Dict[str, Any]) -> None:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    encrypted = encrypt_secret_dpapi(content)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(encrypted)


def load_encrypted_json_file(path: str | Path) -> Dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return {}

    encrypted = target.read_bytes()
    if not encrypted:
        return {}

    plaintext = decrypt_secret_dpapi(encrypted)
    parsed = json.loads(plaintext)
    if not isinstance(parsed, dict):
        raise SecretStoreError("Encrypted config payload is invalid.")
    return parsed


def save_secrets_file(path: str | Path, values: Dict[str, str]) -> None:
    normalized: Dict[str, str] = {}
    for key, value in dict(values).items():
        key_text = str(key).strip()
        if not key_text:
            continue
        normalized[key_text] = "" if value is None else str(value)
    save_encrypted_json_file(path, normalized)


def load_secrets_file(path: str | Path) -> Dict[str, str]:
    parsed = load_encrypted_json_file(path)
    normalized: Dict[str, str] = {}
    for key, value in parsed.items():
        normalized[str(key)] = "" if value is None else str(value)
    return normalized
