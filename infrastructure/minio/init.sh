#!/bin/sh
# Reconcile managed MinIO users/policies in place; preserve other identities.
set -eu
exec python3 "$(dirname "$0")/sync_minio.py" "$@"
