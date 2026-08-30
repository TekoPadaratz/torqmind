#!/usr/bin/env bash
# Bootstrap SSH para deploy TorqMind a partir do Cloud Agent.
# Secrets esperados no dashboard Cursor (Runtime Secret):
#   TM_SSH_PRIVATE_KEY — chave privada ED25519/RSA (recomendado)
#   TM_SSH_PASSWORD    — senha do usuário tm (alternativa; requer sshpass)
set -euo pipefail

mkdir -p "${HOME}/.ssh"
chmod 700 "${HOME}/.ssh"

SSH_CONFIG="${HOME}/.ssh/config"
if ! grep -q "Host torqmind-prod" "${SSH_CONFIG}" 2>/dev/null; then
  cat >> "${SSH_CONFIG}" <<'EOF'

Host torqmind-prod
  HostName redevr.ddns.me
  Port 14022
  User tm
  StrictHostKeyChecking accept-new
  IdentitiesOnly yes
EOF
fi

if [ -n "${TM_SSH_PRIVATE_KEY:-}" ]; then
  printf '%s\n' "${TM_SSH_PRIVATE_KEY}" > "${HOME}/.ssh/torqmind_deploy"
  chmod 600 "${HOME}/.ssh/torqmind_deploy"
  if ! grep -q "IdentityFile.*torqmind_deploy" "${SSH_CONFIG}" 2>/dev/null; then
    sed -i '/^Host torqmind-prod$/a\  IdentityFile ~/.ssh/torqmind_deploy' "${SSH_CONFIG}"
  fi
fi

if [ -n "${TM_SSH_PASSWORD:-}" ] && command -v sshpass >/dev/null 2>&1; then
  printf 'export TM_SSH_PASSWORD\n' > "${HOME}/.torqmind_ssh_env"
  chmod 600 "${HOME}/.torqmind_ssh_env"
fi
