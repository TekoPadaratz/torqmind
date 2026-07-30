#!/usr/bin/env bash
# Valida dominio TorqMind via Cloudflare Tunnel.
# Esperado apos Public Hostname -> http://127.0.0.1:80 : HTTP 200.
set -euo pipefail

fail=0
ok_http() {
  local code="$1"
  [[ "$code" == "200" || "$code" == "301" || "$code" == "302" || "$code" == "307" || "$code" == "308" ]]
}

echo "local origin:"
curl -sS -o /dev/null -w '  127.0.0.1/      %{http_code}\n' --connect-timeout 3 -I 'http://127.0.0.1/' || true
curl -sS -o /dev/null -w '  127.0.0.1/api   %{http_code}\n' --connect-timeout 3 'http://127.0.0.1/api/health' || true
echo "cloudflared: $(systemctl is-active cloudflared 2>/dev/null || echo unknown)"
echo "public:"

www_code=$(curl -sS -o /dev/null -w '%{http_code}' --connect-timeout 12 --max-time 20 'https://www.torqmind.com.br/' || echo 000)
echo "  https://www.torqmind.com.br/ -> $www_code"
ok_http "$www_code" || fail=1

www_api=$(curl -sS -o /dev/null -w '%{http_code}' --connect-timeout 12 --max-time 20 'https://www.torqmind.com.br/api/health' || echo 000)
echo "  https://www.torqmind.com.br/api/health -> $www_api"
ok_http "$www_api" || fail=1

apex_ip=$(dig +short torqmind.com.br A @1.1.1.1 | head -1 || true)
if [[ -z "$apex_ip" ]]; then
  echo "  https://torqmind.com.br/ -> NO_DNS"
  fail=1
else
  apex_code=$(curl -sS -o /dev/null -w '%{http_code}' --connect-timeout 12 --max-time 20 --resolve "torqmind.com.br:443:${apex_ip}" 'https://torqmind.com.br/' || echo 000)
  echo "  https://torqmind.com.br/ (via ${apex_ip}) -> $apex_code"
  ok_http "$apex_code" || fail=1
fi

if [[ "$fail" -ne 0 ]]; then
  echo "FAIL: Zero Trust -> Tunnels -> Public Hostname www+apex -> http://127.0.0.1:80"
  exit 1
fi
echo "PASS"
