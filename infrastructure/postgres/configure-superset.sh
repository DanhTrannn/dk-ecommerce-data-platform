#!/bin/sh
# Explicitly add metadata storage to retained PostgreSQL volumes; never reset source data.
set -eu
: "${SUPERSET_DB_PASSWORD:?set SUPERSET_DB_PASSWORD}"
psql -X -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" --dbname postgres \
  -v bi_db="${SUPERSET_DB_NAME:-superset}" -v bi_user="${SUPERSET_DB_USER:-superset}" \
  -v bi_password="$SUPERSET_DB_PASSWORD" \
  -v source_db="${POSTGRES_ECOMMERCE_DB:-ecommerce}" -v source_user="${POSTGRES_ECOMMERCE_USER:-ecommerce_app}" \
  -v airflow_db="${AIRFLOW_DB_NAME:-airflow}" -v polaris_db="${POLARIS_DB_NAME:-polaris}" <<'SQL'
SELECT :'bi_db' NOT IN ('postgres', 'template0', 'template1', :'source_db', :'airflow_db', :'polaris_db')
  AND :'bi_user' NOT IN (current_user, :'source_user')
  AND NOT EXISTS (SELECT 1 FROM pg_roles r WHERE r.rolname=:'bi_user' AND
    (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls
     OR EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member=r.oid)
     OR EXISTS (SELECT 1 FROM pg_database d WHERE d.datdba=r.oid AND d.datname<>:'bi_db')))
  AND NOT EXISTS (SELECT 1 FROM pg_database d JOIN pg_roles r ON r.oid=d.datdba
                  WHERE d.datname=:'bi_db' AND r.rolname<>:'bi_user') AS safe \gset
\if :safe
\else
  DO $$ BEGIN RAISE EXCEPTION 'Use a separate non-privileged Superset database and owner'; END $$;
\endif
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'bi_user', :'bi_password')
  WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:'bi_user') \gexec
-- Preserve existing role credentials. Changing .env does not rotate a retained role.
SELECT format('CREATE DATABASE %I OWNER %I', :'bi_db', :'bi_user')
  WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname=:'bi_db') \gexec
SELECT format('ALTER ROLE %I SET timezone=''UTC''', :'bi_user') \gexec
SQL
