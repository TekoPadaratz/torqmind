#!/usr/bin/env bash
# CI secret scan with explicit outcomes (never treat infra failure as PASS).
# Exit codes:
#   0 = scanner_ok_clean
#   1 = scanner_ok_findings
#   2 = scanner_unavailable
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

redact() {
  # Avoid echoing high-entropy blobs in logs (portable sed).
  sed -E 's/[A-Za-z0-9_+\/=-]{24,}/[REDACTED]/g'
}

if command -v gitleaks >/dev/null 2>&1; then
  echo "secret_scan_engine=gitleaks"
  set +e
  out="$(gitleaks detect --source "$ROOT" --no-git --redact -v 2>&1)"
  code=$?
  set -e
  printf '%s\n' "$out" | redact
  if [[ "$code" -eq 0 ]]; then
    echo "secret_scan_status=scanner_ok_clean"
    exit 0
  fi
  if [[ "$code" -eq 1 ]]; then
    echo "secret_scan_status=scanner_ok_findings"
    exit 1
  fi
  echo "secret_scan_status=scanner_unavailable"
  exit 2
fi

echo "secret_scan_engine=python_heuristic"
# Lightweight heuristic: flag private-key PEM headers and obvious AWS keys in tracked text.
set +e
hits="$(python3 - <<'PY'
import re, sys
from pathlib import Path
root = Path(".")
patterns = [
    re.compile(r"BEGIN (RSA |OPENSSH |EC )?PRIVATE KEY"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"AGENT_UPDATE_ED25519_PRIVATE_KEY_B64\s*="),
]
allow = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".exe", ".dll", ".so", ".bin"}
findings = []
for path in root.rglob("*"):
    if not path.is_file():
        continue
    if any(part.startswith(".git") for part in path.parts):
        continue
    if path.suffix.lower() in allow:
        continue
    if path.stat().st_size > 1_500_000:
        continue
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        continue
    for pat in patterns:
        if pat.search(text):
            findings.append(str(path))
            break
if findings:
    for f in findings[:50]:
        print(f)
    sys.exit(1)
sys.exit(0)
PY
)"
code=$?
set -e
printf '%s\n' "$hits" | redact
if [[ "$code" -eq 0 ]]; then
  echo "secret_scan_status=scanner_ok_clean"
  exit 0
fi
if [[ "$code" -eq 1 ]]; then
  echo "secret_scan_status=scanner_ok_findings"
  exit 1
fi
echo "secret_scan_status=scanner_unavailable"
exit 2
