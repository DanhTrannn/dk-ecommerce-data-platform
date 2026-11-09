#!/bin/sh
set -eu

connect_url="${DEBEZIUM_CONNECT_URL:-http://debezium-connect:8083}"
connector_name="postgres-ecommerce-cdc"

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

cdc_user="$(json_escape "${POSTGRES_CDC_USER:?POSTGRES_CDC_USER is required}")"
cdc_password="$(json_escape "${POSTGRES_CDC_PASSWORD:?POSTGRES_CDC_PASSWORD is required}")"
database_name="$(json_escape "${POSTGRES_ECOMMERCE_DB:-ecommerce}")"
topic_prefix="$(json_escape "${CDC_TOPIC_PREFIX:-ecommerce}")"

config="$(cat <<JSON
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "database.hostname": "postgres",
  "database.port": "5432",
  "database.user": "${cdc_user}",
  "database.password": "${cdc_password}",
  "topic.prefix": "${topic_prefix}",
  "database.dbname": "${database_name}",
  "plugin.name": "pgoutput",
  "slot.name": "ecommerce_cdc",
  "publication.name": "ecommerce_publication",
  "publication.autocreate.mode": "disabled",
  "schema.include.list": "public",
  "table.include.list": "public\\\\.(customers|categories|products|product_variants|carts|cart_items|wishlist_items|orders|order_items|payments|order_status_history|inventory|coupons|coupon_redemptions|refunds|product_reviews|cities|stores|store_inventory|delivery_staff|shipments|return_requests|return_items|inventory_transactions|inbound_receipts|inbound_receipt_items|shipment_attempts)",
  "snapshot.mode": "initial",
  "provide.transaction.metadata": "true",
  "tombstones.on.delete": "true",
  "decimal.handling.mode": "string",
  "heartbeat.interval.ms": "10000",
  "topic.creation.enable": "true",
  "topic.creation.default.replication.factor": "1",
  "topic.creation.default.partitions": "1",
  "errors.tolerance": "none"
}
JSON
)"

echo "Registering/updating Debezium connector '${connector_name}'..."
curl --fail --silent --show-error \
  -X PUT \
  -H 'Content-Type: application/json' \
  --data "$config" \
  --output /dev/null \
  "${connect_url}/connectors/${connector_name}/config"

attempt=1
while [ "$attempt" -le 30 ]; do
  status="$(curl --fail --silent --show-error "${connect_url}/connectors/${connector_name}/status")"
  if printf '%s' "$status" | grep -Eq '"connector"[^}]*"state"[[:space:]]*:[[:space:]]*"RUNNING"' \
      && printf '%s' "$status" | grep -Eq '"tasks"[[:space:]]*:[[:space:]]*\[[^]]*"state"[[:space:]]*:[[:space:]]*"RUNNING"'; then
    echo "Debezium connector '${connector_name}' and its task are running."
    exit 0
  fi
  sleep 2
  attempt=$((attempt + 1))
done

printf '%s\n' "Connector did not become ready: ${status}" >&2
exit 1
