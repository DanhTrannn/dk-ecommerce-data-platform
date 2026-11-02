#!/usr/bin/env bash
# Start selected service groups; Spark jobs execute locally in their containers.
set -euo pipefail
cd "$(dirname "$0")/.."

PROFILES=core
WAIT_FOR_HEALTH=true
BUILD_IMAGES=false
ALLOW_DEGRADED=false
WAIT_TIMEOUT=180

usage() {
  cat <<'HELP'
Usage: ./scripts/start_all.sh [OPTIONS]

  --profiles LIST   Exact comma-separated groups: core,batch,streaming,tools
                    Default: core (MySQL, API and storefront)
  --all             Select core,batch,streaming,tools
  -b, --build       Build selected images before starting
  --no-wait         Skip readiness and Gold-schema checks
  --allow-degraded  Continue after readiness/schema failure with a warning
  -h, --help        Show help

Examples:
  ./scripts/start_all.sh
  ./scripts/start_all.sh --profiles core,batch --build
  ./scripts/start_all.sh --profiles core,batch,streaming
  ./scripts/start_all.sh --all

Selection overrides COMPOSE_PROFILES, including an old value in .env.
Existing containers from other groups remain running. Use make down before
starting a smaller stack if those containers should also be stopped.
HELP
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profiles)
      if [[ $# -lt 2 || -z "$2" ]]; then
        echo "--profiles requires a comma-separated list." >&2
        exit 2
      fi
      PROFILES="$2"; shift 2 ;;
    --all) PROFILES=core,batch,streaming,tools; shift ;;
    -b|--build) BUILD_IMAGES=true; shift ;;
    --no-wait) WAIT_FOR_HEALTH=false; shift ;;
    --allow-degraded) ALLOW_DEGRADED=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ "$PROFILES" == ,* || "$PROFILES" == *, || "$PROFILES" == *,,* ]]; then
  echo "Invalid profile list: $PROFILES" >&2
  exit 2
fi
IFS=',' read -r -a SELECTED_PROFILES <<<"$PROFILES"
for profile in "${SELECTED_PROFILES[@]}"; do
  case "$profile" in
    core|batch|streaming|tools) ;;
    *) echo "Unknown profile: $profile" >&2; exit 2 ;;
  esac
done
has_profile() { [[ ",$PROFILES," == *",$1,"* ]]; }

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example; review its local development settings."
fi

# Shell environment overrides Compose's .env without sourcing secrets as code.
export COMPOSE_PROFILES="$PROFILES"
if has_profile streaming; then
  export KAFKA_ENABLED=true
else
  export KAFKA_ENABLED=false
fi

UP_ARGS=(-d)
if [[ "$BUILD_IMAGES" == true ]]; then UP_ARGS+=(--build); fi
if [[ "$WAIT_FOR_HEALTH" == true ]]; then
  UP_ARGS+=(--wait --wait-timeout "$WAIT_TIMEOUT")
fi
DEGRADED=false
echo "Starting service groups: $PROFILES (Kafka access logging: $KAFKA_ENABLED)"
if ! docker compose up "${UP_ARGS[@]}"; then
  if [[ "$ALLOW_DEGRADED" != true ]]; then
    echo "Startup/readiness failed. Inspect make ps and make logs." >&2
    exit 1
  fi
  DEGRADED=true
  echo "WARNING: startup/readiness failed; continuing with --allow-degraded." >&2
fi

# Schema creation applies only to data groups, never web-only startup.
# It does not populate analytical results or trigger batch processing.
if [[ "$WAIT_FOR_HEALTH" == true ]] && { has_profile batch || has_profile streaming; }; then
  gold_schema_exists() {
    local output
    output="$(docker compose exec -T trino trino --execute \
      "SHOW TABLES FROM lakehouse.gold LIKE 'mart_sales_daily'" 2>/dev/null)" || return 1
    [[ "$output" == *mart_sales_daily* ]]
  }
  if ! gold_schema_exists; then
    echo "Initializing missing Gold table schemas with local Spark SQL..."
    if ! docker compose run --rm --no-deps spark-client \
      /opt/spark/bin/spark-sql -f /opt/project/spark/sql/init_gold_marts.sql || ! gold_schema_exists; then
      if [[ "$ALLOW_DEGRADED" != true ]]; then
        echo "Gold schema initialization/verification failed." >&2
        exit 1
      fi
      DEGRADED=true
      echo "WARNING: Gold schemas could not be verified." >&2
    fi
  fi
fi

# Resolve published ports from the selected Compose model, including .env overrides.
show_url() {
  local label="$1" service="$2" port="$3" path="${4:-}" binding
  binding="$(docker compose port "$service" "$port" 2>/dev/null)" || return 0
  [[ -n "$binding" ]] || return 0
  printf '  %-24s http://localhost:%s%s\n' "$label" "${binding##*:}" "$path"
}
echo
echo "Selected service endpoints:"
if has_profile core; then
  show_url Storefront storefront 3000
  show_url 'BI dashboards' storefront 3000 /admin/analytics
  show_url 'API documentation' ecommerce-api 8000 /docs
fi
if has_profile batch; then show_url Airflow airflow-webserver 8080; fi
if has_profile batch || has_profile streaming || has_profile tools; then
  show_url Trino trino 8080
  show_url 'MinIO Console' minio 9001
fi
if has_profile streaming; then
  show_url Flink flink-jobmanager 8081
  show_url 'Debezium Connect' debezium-connect 8083
fi
if has_profile tools; then
  show_url 'Polaris Console' polaris-console 8080
  show_url 'LibreDB Studio' libredb-studio 3000
  show_url 'Trino Admin' trino-admin 8080
fi
if [[ "$DEGRADED" == true ]]; then
  echo "WARNING: selected stack is degraded; inspect service logs."
elif [[ "$WAIT_FOR_HEALTH" == false ]]; then
  echo "Startup requested; readiness and schemas were not checked."
else
  echo "Selected services passed Compose readiness checks."
fi
echo 'Credentials: see .env. Status: make ps. Logs: make logs. Stop all: make down.'
