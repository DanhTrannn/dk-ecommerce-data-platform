#!/bin/sh
set -eu

credential_file="${POLARIS_CREDENTIAL_FILE:-/run/polaris/clients.env}"
attempt=0
until [ -s "$credential_file" ]; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 120 ]; then
    echo "Polaris credential file is not ready: $credential_file" >&2
    exit 1
  fi
  sleep 1
done

# shellcheck disable=SC1090
. "$credential_file"

: "${POLARIS_TRINO_CLIENT_ID:?POLARIS_TRINO_CLIENT_ID is required}"
: "${POLARIS_TRINO_CLIENT_SECRET:?POLARIS_TRINO_CLIENT_SECRET is required}"
export POLARIS_CLIENT_ID="$POLARIS_TRINO_CLIENT_ID"
export POLARIS_CLIENT_SECRET="$POLARIS_TRINO_CLIENT_SECRET"

# A separate workload file backs the writer catalog; BI cannot access it through SQL ACLs.
. /run/polaris-writer/clients.env
export POLARIS_WRITER_CLIENT_ID="$POLARIS_BATCH_CLIENT_ID"
export POLARIS_WRITER_CLIENT_SECRET="$POLARIS_BATCH_CLIENT_SECRET"
exec /usr/lib/trino/bin/run-trino
