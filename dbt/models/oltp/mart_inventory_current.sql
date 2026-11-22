-- Stock per position on the as-of day; average sales over the 28 days ending that day.
-- Rebuilt every run: it is a snapshot of one day, so every row changes daily.
WITH recent AS (
  SELECT location_type, store_key, variant_key, sum(sold_online_qty + sold_pos_qty) AS sold_28d
  FROM {{ live('fact_inventory_daily') }}
  WHERE inventory_date > date_add('day', -28, {{ business_date() }})
  GROUP BY 1, 2, 3
),
last_sale AS (
  SELECT location_type, store_key, variant_key, max(inventory_date) AS last_sale_date
  FROM {{ live('fact_inventory_daily') }}
  WHERE sold_online_qty + sold_pos_qty > 0
  GROUP BY 1, 2, 3
)
SELECT
  f.inventory_date,
  f.location_type,
  f.store_key,
  s.store_name,
  f.variant_key,
  v.sku,
  f.product_key,
  p.product_name,
  p.category_name,
  f.closing_qty AS on_hand_qty,
  f.closing_value_vnd AS stock_value_vnd,
  coalesce(r.sold_28d, 0) / 28e0 AS avg_daily_sales_28d,
  CASE WHEN coalesce(r.sold_28d, 0) > 0 THEN f.closing_qty / (r.sold_28d / 28e0) END AS days_of_cover,
  l.last_sale_date
FROM {{ live('fact_inventory_daily') }} f
LEFT JOIN recent r
  ON r.location_type = f.location_type AND r.store_key IS NOT DISTINCT FROM f.store_key AND r.variant_key = f.variant_key
LEFT JOIN last_sale l
  ON l.location_type = f.location_type AND l.store_key IS NOT DISTINCT FROM f.store_key AND l.variant_key = f.variant_key
LEFT JOIN {{ live('dim_store') }} s ON s.store_key = f.store_key
LEFT JOIN {{ live('dim_variant') }} v ON v.variant_key = f.variant_key
LEFT JOIN {{ live('dim_product') }} p ON p.product_key = f.product_key
WHERE f.inventory_date = {{ business_date() }}
