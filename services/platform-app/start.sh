#!/usr/bin/env bash
set -eu

# Stop both services when either exits or Docker stops the container.
trap 'kill "$api_pid" "$console_pid" 2>/dev/null || true; wait || true' EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
api_pid=""
console_pid=""

uvicorn platform_app.api.main:app --host 0.0.0.0 --port 8000 --no-access-log &
api_pid=$!
platform-app --server.address=0.0.0.0 --server.port=8501 &
console_pid=$!

set +e
wait -n "$api_pid" "$console_pid"
exit $?
