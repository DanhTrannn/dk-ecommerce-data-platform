SELECT
  CAST(b."summary_key" AS varchar) AS "summary_key",
  CAST(b."product_rows" AS bigint) AS "product_rows",
  CAST(b."variant_rows" AS bigint) AS "variant_rows",
  CAST(b."inventory_rows" AS bigint) AS "inventory_rows",
  CAST(b."customer_rows" AS bigint) AS "customer_rows",
  CAST(b."review_rows" AS bigint) AS "review_rows",
  CAST(b."coupon_rows" AS bigint) AS "coupon_rows",
  CAST(b."order_rows" AS bigint) AS "order_rows",
  CAST(b."active_products" AS bigint) AS "active_products",
  CAST(b."active_variants" AS bigint) AS "active_variants",
  CAST(b."low_stock_variants" AS bigint) AS "low_stock_variants",
  CAST(b."customers" AS bigint) AS "customers",
  CAST(b."total_reviews" AS bigint) AS "total_reviews",
  CAST(b."active_coupons" AS bigint) AS "active_coupons",
  CAST(b."pending_payment_orders" AS bigint) AS "pending_payment_orders",
  CAST(b."paid_orders" AS bigint) AS "paid_orders",
  CAST(b."confirmed_orders" AS bigint) AS "confirmed_orders",
  CAST(b."shipping_orders" AS bigint) AS "shipping_orders",
  CAST(b."delivered_orders" AS bigint) AS "delivered_orders",
  CAST(b."completed_orders" AS bigint) AS "completed_orders",
  CAST(b."cancelled_orders" AS bigint) AS "cancelled_orders",
  CAST(b."failed_delivery_orders" AS bigint) AS "failed_delivery_orders",
  CAST(b."returned_orders" AS bigint) AS "returned_orders",
  CAST(b."admin_observed_at" AS timestamp(6) with time zone) AS "admin_observed_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
WITH p AS (SELECT count(*) product_rows,sum((CASE WHEN is_active THEN 1 ELSE 0 END)) active_products FROM {{ pinned('products') }}),
        v AS (SELECT count(*) variant_rows FROM {{ pinned('product_variants') }}),active AS (SELECT v.variant_id FROM {{ pinned('product_variants') }} v
        JOIN {{ pinned('products') }} p ON v.product_id=p.product_id WHERE v.is_active AND p.is_active),
        ac AS (SELECT count(*) active_variants FROM active),i AS (SELECT count(*) inventory_rows FROM {{ ref('fact_inventory_current') }} WHERE location_type='warehouse'),
        low AS (SELECT count(*) low_stock_variants FROM {{ ref('fact_inventory_current') }} i JOIN active a ON i.variant_key=a.variant_id
        WHERE i.location_type='warehouse' AND i.is_low_stock),c AS (SELECT count(*) customer_rows,
        sum((CASE WHEN role='customer' THEN 1 ELSE 0 END)) customers FROM {{ pinned('customers') }}),r AS (SELECT count(*) review_rows FROM {{ pinned('product_reviews') }}),
        q AS (SELECT count(*) coupon_rows,sum((CASE WHEN is_active AND archived_at IS NULL AND starts_at<=observed_at
        AND ends_at>observed_at THEN 1 ELSE 0 END)) active_coupons FROM {{ pinned('coupons') }} CROSS JOIN {{ clock() }} clock),o AS (
        SELECT count(*) order_rows,
        sum((CASE WHEN status='pending_payment' THEN 1 ELSE 0 END)) pending_payment_orders,sum((CASE WHEN status='paid' THEN 1 ELSE 0 END)) paid_orders,
        sum((CASE WHEN status='confirmed' THEN 1 ELSE 0 END)) confirmed_orders,sum((CASE WHEN status='shipping' THEN 1 ELSE 0 END)) shipping_orders,
        sum((CASE WHEN status='delivered' THEN 1 ELSE 0 END)) delivered_orders,sum((CASE WHEN status='completed' THEN 1 ELSE 0 END)) completed_orders,
        sum((CASE WHEN status='cancelled' THEN 1 ELSE 0 END)) cancelled_orders,sum((CASE WHEN status='failed_delivery' THEN 1 ELSE 0 END)) failed_delivery_orders,
        sum((CASE WHEN status='returned' THEN 1 ELSE 0 END)) returned_orders FROM {{ ref('fact_order') }})
        SELECT 'current' summary_key,p.*,v.variant_rows,i.inventory_rows,c.*,r.review_rows,q.*,o.*,
        CASE WHEN p.product_rows>0 AND v.variant_rows>0 THEN ac.active_variants END active_variants,
        CASE WHEN p.product_rows>0 AND v.variant_rows>0 AND i.inventory_rows>0 THEN low.low_stock_variants END low_stock_variants,
        CASE WHEN r.review_rows>0 THEN r.review_rows END total_reviews,clock.observed_at admin_observed_at
        FROM p CROSS JOIN v CROSS JOIN i CROSS JOIN c CROSS JOIN r CROSS JOIN q CROSS JOIN o CROSS JOIN ac CROSS JOIN low CROSS JOIN {{ clock() }} clock
) b
