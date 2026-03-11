#!/usr/bin/env bash
set -euo pipefail

SRC_CONFIG="${1:-/opt/yap/app/config/caddy/Caddyfile}"
DEST_CONFIG="${2:-/etc/caddy/Caddyfile}"
BACKUP_PATH="${DEST_CONFIG}.bak_$(date +%Y%m%d_%H%M%S)"

if [[ ! -f "${SRC_CONFIG}" ]]; then
  echo "[error] Source config not found: ${SRC_CONFIG}" >&2
  exit 1
fi

echo "[info] Backup current Caddyfile -> ${BACKUP_PATH}"
sudo cp -f "${DEST_CONFIG}" "${BACKUP_PATH}"

echo "[info] Applying ${SRC_CONFIG} -> ${DEST_CONFIG}"
sudo cp -f "${SRC_CONFIG}" "${DEST_CONFIG}"

echo "[info] Validating config"
sudo caddy validate --config "${DEST_CONFIG}"

echo "[info] Reloading caddy"
sudo systemctl reload caddy
sudo systemctl --no-pager --full status caddy | sed -n '1,25p'

echo "[ok] Caddy sync completed."
