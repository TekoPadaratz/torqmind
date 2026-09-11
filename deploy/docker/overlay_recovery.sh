#!/usr/bin/env bash
# Overlay dual-verify security.py + ensure /auth/me rejects MFA intermediate tokens.
# Targets both /app/app and site-packages (pip install . layout).
set -euo pipefail
SRC="${1:?security_dual_verify.py path required}"

python - "$SRC" <<'PY'
from __future__ import annotations

import shutil
import sys
from pathlib import Path

src = Path(sys.argv[1])
assert src.is_file(), src

targets: set[Path] = {
    Path("/app/app/security.py"),
    Path("/usr/local/lib/python3.11/site-packages/app/security.py"),
}
try:
    import app

    targets.add(Path(app.__file__).resolve().parent / "security.py")
except Exception:
    pass
try:
    import importlib.util

    spec = importlib.util.find_spec("app.security")
    if spec and spec.origin:
        targets.add(Path(spec.origin))
except Exception:
    pass

for dest in sorted(targets):
    if not dest.parent.exists() and dest.as_posix().startswith("/usr/local/lib/"):
        print("skip_missing_parent", dest)
        continue
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dest)
    print("overlayed_security", dest)

ME_GATE_LINES = [
    "    if payload.get(\"scope\") in {\"mfa_challenge\", \"mfa_setup\"} or payload.get(\"mfa_pending\"):",
    "        raise HTTPException(",
    "            status_code=401,",
    "            detail={\"error\": \"mfa_required\", \"message\": \"Two-factor authentication required.\"},",
    "        )",
]


def patch_me(text: str) -> str:
    if 'detail={"error": "mfa_required"' in text or "detail={'error': 'mfa_required'" in text:
        if "def me(" in text and "mfa_pending" in text:
            return text  # already gated
    lines = text.splitlines(keepends=True)
    me_idx = None
    for i, line in enumerate(lines):
        if line.startswith("def me("):
            me_idx = i
            break
    if me_idx is None:
        raise RuntimeError("def me not found")

    # Find decode try/except ending at raise ... invalid_token
    insert_at = None
    i = me_idx
    while i < len(lines) and not (lines[i].startswith("def ") and i > me_idx):
        if "payload = decode_token(token)" in lines[i]:
            # walk to matching except raise invalid_token
            j = i + 1
            while j < len(lines):
                if "invalid_token" in lines[j] and "HTTPException" in lines[j]:
                    insert_at = j + 1
                    # if raise spans multiple lines, consume until balanced-ish next blank/non-indent break
                    if lines[j].rstrip().endswith(","):
                        k = j + 1
                        while k < len(lines) and lines[k].strip() and not lines[k].lstrip().startswith(("user_id", "try:", "if ", "return", "#")):
                            if lines[k].strip() == ")":
                                insert_at = k + 1
                                break
                            k += 1
                    break
                if lines[j].startswith("def "):
                    break
                j += 1
            break
        i += 1
    if insert_at is None:
        raise RuntimeError("decode try/except anchor not found in me()")

    gate = [ln + "\n" for ln in ME_GATE_LINES]
    # Ensure a blank line after gate before user_id
    if insert_at < len(lines) and lines[insert_at].strip():
        gate.append("\n")
    return "".join(lines[:insert_at] + gate + lines[insert_at:])


route_targets: set[Path] = {
    Path("/app/app/routes_auth.py"),
    Path("/usr/local/lib/python3.11/site-packages/app/routes_auth.py"),
}
try:
    import app.routes_auth as ra

    route_targets.add(Path(ra.__file__).resolve())
except Exception:
    pass

for dest in sorted(route_targets):
    if not dest.is_file():
        print("skip_missing_routes_auth", dest)
        continue
    original = dest.read_text(encoding="utf-8")
    patched = patch_me(original)
    if patched == original:
        print("me_gate_already_present", dest)
        continue
    dest.write_text(patched, encoding="utf-8")
    # Sanity compile
    compile(patched, str(dest), "exec")
    print("overlayed_me_gate", dest)
PY
