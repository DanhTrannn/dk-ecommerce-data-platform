#!/bin/bash
set -eu

create_user_and_database() {
  local database=$1 user=$2 password=$3
  echo "Creating database '$database' and owner '$user'..."
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    -v db_name="$database" -v db_user="$user" -v db_password="$password" <<'SQL'
CREATE USER :"db_user" WITH PASSWORD :'db_password';
CREATE DATABASE :"db_name" OWNER :"db_user";
SQL
}

create_user_and_database "${POSTGRES_ECOMMERCE_DB:-ecommerce}" "${POSTGRES_ECOMMERCE_USER:-ecommerce_app}" "${POSTGRES_ECOMMERCE_PASSWORD:-password}"
create_user_and_database "${POLARIS_DB_NAME:-polaris}" "${POLARIS_DB_USER:-polaris}" "${POLARIS_DB_PASSWORD:-password}"
create_user_and_database "${AIRFLOW_DB_NAME:-airflow}" "${AIRFLOW_DB_USER:-airflow}" "${AIRFLOW_DB_PASSWORD:-password}"

# Polaris 1.8 requires the schema to exist before its realm bootstrap.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "${POLARIS_DB_NAME:-polaris}" \
  -v polaris_user="${POLARIS_DB_USER:-polaris}" <<'SQL'
CREATE SCHEMA polaris_schema AUTHORIZATION :"polaris_user";
SQL
