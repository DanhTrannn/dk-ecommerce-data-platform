#!/bin/sh
# Create the lakehouse bucket and the storage account Polaris uses to vend credentials. Every step is idempotent.
set -eu

mc alias set lakehouse http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
mc mb --ignore-existing "lakehouse/$MINIO_LAKEHOUSE_BUCKET"
sed "s/__MINIO_BUCKET__/$MINIO_LAKEHOUSE_BUCKET/g" /opt/minio/policies/lakehouse-polaris.json > /tmp/lakehouse-polaris.json
mc admin policy create lakehouse lakehouse-polaris /tmp/lakehouse-polaris.json
mc admin user add lakehouse "$MINIO_POLARIS_ACCESS_KEY" "$MINIO_POLARIS_SECRET_KEY"
if ! mc admin user info lakehouse "$MINIO_POLARIS_ACCESS_KEY" | grep -q "PolicyName: lakehouse-polaris"; then
  mc admin policy attach lakehouse lakehouse-polaris --user "$MINIO_POLARIS_ACCESS_KEY"
fi
