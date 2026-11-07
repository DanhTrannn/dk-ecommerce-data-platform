#!/bin/sh
# Explicitly provision a dedicated reader after the business schema exists.
set -eu
: "${POSTGRES_ECOMMERCE_READER_USER:?set POSTGRES_ECOMMERCE_READER_USER}"
: "${POSTGRES_ECOMMERCE_READER_PASSWORD:?set POSTGRES_ECOMMERCE_READER_PASSWORD}"

psql -X -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" \
  --dbname "${POSTGRES_ECOMMERCE_DB:-ecommerce}" \
  -v reader_user="$POSTGRES_ECOMMERCE_READER_USER" \
  -v reader_password="$POSTGRES_ECOMMERCE_READER_PASSWORD" <<'SQL'
BEGIN;
CREATE TEMP TABLE analytical_sources (name text PRIMARY KEY) ON COMMIT DROP;
INSERT INTO analytical_sources VALUES
('customers'),('categories'),('products'),('product_variants'),('carts'),
('cart_items'),('wishlist_items'),('orders'),('order_items'),('payments'),
('order_status_history'),('inventory'),('coupons'),('coupon_redemptions'),
('refunds'),('product_reviews'),('cities'),('stores'),('store_inventory'),
('delivery_staff'),('shipments'),('return_requests'),('return_items'),
('inventory_transactions'),('inbound_receipts'),('inbound_receipt_items'),('shipment_attempts');
-- Fail before changing any privileges if bootstrap is incomplete.
SELECT count(*) = 27 AS schema_ready FROM analytical_sources
WHERE to_regclass(format('public.%I', name)) IS NOT NULL \gset
\if :schema_ready
\else
  \echo 'Apply the complete business schema before configuring the source reader.'
  DO $$ BEGIN RAISE EXCEPTION 'Source reader prerequisites not met'; END $$;
\endif
SELECT NOT EXISTS (
  SELECT 1 FROM pg_roles r WHERE r.rolname=:'reader_user' AND (
    r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls
    OR EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member=r.oid)
    OR EXISTS (SELECT 1 FROM pg_database d WHERE d.datdba=r.oid)
    OR EXISTS (SELECT 1 FROM pg_class c WHERE c.relowner=r.oid)
    OR EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspowner=r.oid)
  )
) AS role_safe \gset
\if :role_safe
\else
  \echo 'Reader must be a dedicated non-owner role without elevated privileges or memberships.'
  DO $$ BEGIN RAISE EXCEPTION 'Source reader prerequisites not met'; END $$;
\endif
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'reader_user', :'reader_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:'reader_user') \gexec
ALTER ROLE :"reader_user" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
ALTER ROLE :"reader_user" PASSWORD :'reader_password';
ALTER ROLE :"reader_user" SET timezone='UTC';
ALTER ROLE :"reader_user" SET default_transaction_read_only=on;
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'reader_user') \gexec
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM :"reader_user";
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM :"reader_user";
REVOKE CREATE ON SCHEMA public FROM :"reader_user";
GRANT USAGE ON SCHEMA public TO :"reader_user";
SELECT format('GRANT SELECT ON TABLE public.%I TO %I', name, :'reader_user')
FROM analytical_sources ORDER BY name \gexec
-- Do not grant default privileges: future tables need an explicit review.
COMMIT;
SQL
