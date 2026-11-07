#!/bin/sh
# Explicit first-time IAM setup using mc already bundled with the server image.
set -eu
umask 077
: "${MINIO_ROOT_USER:?set MINIO_ROOT_USER}"
: "${MINIO_ROOT_PASSWORD:?set MINIO_ROOT_PASSWORD}"
: "${MINIO_LAKEHOUSE_BUCKET:?set MINIO_LAKEHOUSE_BUCKET}"

fail() { echo "MinIO provisioning failed: $1" >&2; exit 1; }
case "$MINIO_LAKEHOUSE_BUCKET" in *[!a-z0-9.-]*|'') fail 'invalid bucket name';; esac
lock=/tmp/lakehouse-provision.lock
mkdir "$lock" 2>/dev/null || fail 'another provisioning operation is active or a stale lock needs inspection'
work=$(mktemp -d)
trap 'rm -rf "$work" "$lock"' EXIT HUP INT TERM
export MC_CONFIG_DIR="$work/config"
# Do not expose mc errors, aliases or credentials in logs.
quiet() { mc --json "$@" >"$work/result" 2>&1; }
quiet alias set bootstrap http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" || fail 'root authentication'
quiet mb --ignore-existing "bootstrap/$MINIO_LAKEHOUSE_BUCKET" || fail 'bucket initialization'

ensure_identity() {
    kind=$1 access=$2 secret=$3
    case "$access" in *[!A-Za-z0-9._-]*|'') fail 'invalid workload access key';; esac
    [ -n "$secret" ] || fail 'workload secret is missing'
    [ "$access" != "$MINIO_ROOT_USER" ] && [ "$secret" != "$MINIO_ROOT_PASSWORD" ] || fail 'workload must not reuse root credentials'
    policy="lakehouse-$kind"
    if ! quiet admin policy info bootstrap "$policy"; then
        grep -q '"Code":"XMinioAdminNoSuchPolicy"' "$work/result" || fail 'policy lookup'
        sed "s/__MINIO_BUCKET__/$MINIO_LAKEHOUSE_BUCKET/g" \
            "/opt/project/minio/policies/$policy.json" >"$work/policy.json"
        quiet admin policy create bootstrap "$policy" "$work/policy.json" || fail 'policy creation'
    fi
    if ! quiet admin user info bootstrap "$access"; then
        grep -q '"Code":"XMinioAdminNoSuchUser"' "$work/result" || fail 'user lookup'
        quiet admin user add bootstrap "$access" "$secret" || fail 'user creation'
        quiet admin policy attach bootstrap "$policy" --user "$access" || fail 'policy assignment; inspect the new account before rerunning'
    fi
    # Existing users/policies and their manual grants are never overwritten.
    # This validates the supplied secret and bucket/prefix read access, not all IAM grants.
    quiet alias set workload http://127.0.0.1:9000 "$access" "$secret" || fail 'workload authentication; no credentials were reset'
    quiet ls "workload/$MINIO_LAKEHOUSE_BUCKET/warehouse/" || fail 'workload access; inspect existing credentials/policies manually'
    echo "MinIO identity ready: $kind"
}

# Validate all keys first; do not accidentally attach multiple workload scopes to one user.
[ "$(printf '%s\n' "${MINIO_POLARIS_ACCESS_KEY:?}" "${MINIO_SPARK_ACCESS_KEY:?}" "${MINIO_FLINK_ACCESS_KEY:?}" "${MINIO_TRINO_ACCESS_KEY:?}" | sort -u | wc -l)" -eq 4 ] || fail 'workload identities must be distinct'
ensure_identity polaris "$MINIO_POLARIS_ACCESS_KEY" "${MINIO_POLARIS_SECRET_KEY:?}"
ensure_identity spark "$MINIO_SPARK_ACCESS_KEY" "${MINIO_SPARK_SECRET_KEY:?}"
ensure_identity flink "$MINIO_FLINK_ACCESS_KEY" "${MINIO_FLINK_SECRET_KEY:?}"
ensure_identity trino "$MINIO_TRINO_ACCESS_KEY" "${MINIO_TRINO_SECRET_KEY:?}"
echo 'MinIO provisioning complete; existing data and IAM settings retained.'
