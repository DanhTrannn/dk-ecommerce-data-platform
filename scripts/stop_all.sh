#!/usr/bin/env bash
# Stop every service group without removing data volumes by default.
set -euo pipefail
cd "$(dirname "$0")/.."
echo "Stopping all platform service groups..."
docker compose --profile '*' down "$@"
echo "All service groups stopped."
