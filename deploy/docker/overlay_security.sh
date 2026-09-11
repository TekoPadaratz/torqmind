#!/usr/bin/env bash
# Overlay security.py onto /app/app and site-packages/app (pip install target).
set -euo pipefail
SRC="${1:?security.py path required}"
python - "$SRC" <<'PY'
from pathlib import Path
import shutil
import sys
import app

src = Path(sys.argv[1])
targets = {
    Path("/app/app/security.py"),
    Path(app.__file__).resolve().parent / "security.py",
    Path("/usr/local/lib/python3.11/site-packages/app/security.py"),
}
# Prefer whatever path Python would import *without* relying on cwd alone.
try:
    import importlib.util

    spec = importlib.util.find_spec("app.security")
    if spec and spec.origin:
        targets.add(Path(spec.origin))
except Exception:
    pass

for dest in sorted(targets):
    if not dest.parent.exists() and dest.as_posix().startswith("/usr/local/lib/"):
        # site-packages package must already exist from base image pip install
        print("skip_missing_parent", dest)
        continue
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dest)
    print("overlayed", dest)
PY
