#!/bin/sh
# Explicit post-schema setup; never resets replication slots or Kafka offsets.
set -eu
: "${POSTGRES_CDC_USER:?set POSTGRES_CDC_USER}"
: "${POSTGRES_CDC_PASSWORD:?set POSTGRES_CDC_PASSWORD}"

psql -X -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" \
  --dbname "${POSTGRES_ECOMMERCE_DB:-ecommerce}" \
  -v app_user="${POSTGRES_ECOMMERCE_USER:-ecommerce_app}" \
  -v cdc_user="$POSTGRES_CDC_USER" -v cdc_password="$POSTGRES_CDC_PASSWORD" <<'SQL'
BEGIN;
CREATE TEMP TABLE analytical_sources (name text PRIMARY KEY) ON COMMIT DROP;
INSERT INTO analytical_sources VALUES
('customers'),('categories'),('products'),('product_variants'),('carts'),
('cart_items'),('wishlist_items'),('orders'),('order_items'),('payments'),
('order_status_history'),('inventory'),('coupons'),('coupon_redemptions'),
('refunds'),('product_reviews'),('cities'),('stores'),('store_inventory'),
('delivery_staff'),('shipments'),('return_requests'),('return_items'),
('inventory_transactions'),('inbound_receipts'),('inbound_receipt_items'),('shipment_attempts');
SELECT count(*) = 27 AS schema_ready FROM analytical_sources
WHERE to_regclass(format('public.%I', name)) IS NOT NULL \gset
\if :schema_ready
\else
  \echo 'Apply the complete business schema before configuring CDC.'
  DO $$ BEGIN RAISE EXCEPTION 'Source CDC prerequisites not met'; END $$;
\endif
-- Do not repurpose an owner/admin or a role with unrelated inherited access.
SELECT :'cdc_user' <> :'app_user' AND :'cdc_user' <> current_user AND NOT EXISTS (
  SELECT 1 FROM pg_roles r WHERE r.rolname=:'cdc_user' AND (
    r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolbypassrls
    OR EXISTS (SELECT 1 FROM pg_auth_members m JOIN pg_roles p ON p.oid=m.roleid
               WHERE m.member=r.oid AND p.rolname<>:'app_user')
    OR EXISTS (SELECT 1 FROM pg_database d WHERE d.datdba=r.oid)
    OR EXISTS (SELECT 1 FROM pg_class c WHERE c.relowner=r.oid)
    OR EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspowner=r.oid)
  )
) AS role_safe \gset
\if :role_safe
\else
  \echo 'CDC must be a dedicated non-owner role without unrelated memberships.'
  DO $$ BEGIN RAISE EXCEPTION 'Source CDC prerequisites not met'; END $$;
\endif
SELECT format('CREATE ROLE %I LOGIN REPLICATION PASSWORD %L', :'cdc_user', :'cdc_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:'cdc_user') \gexec
ALTER ROLE :"cdc_user" LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
ALTER ROLE :"cdc_user" PASSWORD :'cdc_password';
ALTER ROLE :"cdc_user" SET timezone='UTC';
-- Remove the source-owner membership used by the former filtered auto-publication setup.
SELECT format('REVOKE %I FROM %I', :'app_user', :'cdc_user')
WHERE pg_has_role(:'cdc_user', :'app_user', 'MEMBER') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'cdc_user') \gexec
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM :"cdc_user";
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM :"cdc_user";
REVOKE CREATE ON SCHEMA public FROM :"cdc_user";
GRANT USAGE ON SCHEMA public TO :"cdc_user";
SELECT format('GRANT SELECT ON TABLE public.%I TO %I', name, :'cdc_user')
FROM analytical_sources ORDER BY name \gexec
SELECT format('ALTER TABLE public.%I REPLICA IDENTITY FULL', name)
FROM analytical_sources ORDER BY name \gexec
SELECT format('CREATE PUBLICATION ecommerce_publication FOR TABLE %s',
              string_agg(format('public.%I', name), ', ' ORDER BY name))
FROM analytical_sources WHERE NOT EXISTS (
  SELECT 1 FROM pg_publication WHERE pubname='ecommerce_publication'
) HAVING count(*)=27 \gexec
SELECT format('ALTER PUBLICATION ecommerce_publication SET TABLE %s',
              string_agg(format('public.%I', name), ', ' ORDER BY name))
FROM analytical_sources \gexec
COMMIT;
SQL
