#!/bin/sh
set -eu

# Avoid apk TLS hang in Docker bridge and terminal progress wait
sed -i 's|https://|http://|g' /etc/apk/repositories
apk add --no-cache --no-progress python3 py3-yaml

POLARIS_URL="${POLARIS_URL:-http://polaris:8181}"
REALM="${POLARIS_REALM:-POLARIS}"
CREDENTIAL_STATE_FILE="${POLARIS_CREDENTIAL_STATE_FILE:-/run/polaris-state/clients.env}"
READY_FILE="${POLARIS_READY_FILE:-/run/polaris-state/ready}"
CONFIG_FILE="${POLARIS_RBAC_CONFIG:-/bootstrap/rbac.yml}"
echo "Checking Polaris bootstrap state (YAML is only a first-run seed)..."
exec python3 /bootstrap/sync_polaris_rbac.py \
  --config "${CONFIG_FILE}" \
  --polaris-url "${POLARIS_URL}" \
  --realm "${REALM}" \
  --credential-state-file "${CREDENTIAL_STATE_FILE}" \
  --ready-file "${READY_FILE}" \
  "$@"
