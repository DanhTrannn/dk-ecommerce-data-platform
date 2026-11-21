-- Each position's as-of closing stock equals the sum of its ledger movements up to the as-of time.
WITH ledger AS (
  SELECT location_type, store_id AS store_key, variant_id AS variant_key, sum(quantity_delta) AS balance
  FROM {{ ref('inventory_transactions') }}
  WHERE created_at <= {{ as_of() }}
  GROUP BY 1, 2, 3
),
latest AS (
  SELECT location_type, store_key, variant_key, closing_qty
  FROM {{ ref('fact_inventory_daily') }}
  WHERE inventory_date = {{ business_date() }}
)
SELECT *
FROM ledger l
FULL JOIN latest f
  ON f.location_type = l.location_type AND f.store_key IS NOT DISTINCT FROM l.store_key AND f.variant_key = l.variant_key
WHERE l.balance IS DISTINCT FROM f.closing_qty
