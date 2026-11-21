{{ config(properties={"partitioning": "ARRAY['month(inventory_date)']"}) }}
-- One row per day for each stock position (central warehouse or store x variant), from its first movement up
-- to the as-of day. Built from the stock ledger only: a position's balance is the running sum of its movements.
WITH movements AS (
  SELECT
    {{ vn_date('created_at') }} AS movement_date,
    location_type,
    store_id AS store_key,
    variant_id AS variant_key,
    sum(CASE WHEN movement_type = 'inbound' THEN quantity_delta ELSE 0 END) AS inbound_qty,
    -sum(CASE WHEN movement_type = 'outbound_order' THEN quantity_delta ELSE 0 END) AS sold_online_qty,
    -sum(CASE WHEN movement_type = 'outbound_pos' THEN quantity_delta ELSE 0 END) AS sold_pos_qty,
    -sum(CASE WHEN movement_type = 'transfer_to_store' THEN quantity_delta ELSE 0 END) AS transfer_out_qty,
    sum(CASE WHEN movement_type = 'transfer_received' THEN quantity_delta ELSE 0 END) AS transfer_in_qty,
    sum(CASE WHEN movement_type IN ('return_customer', 'return_boom', 'order_cancellation') THEN quantity_delta ELSE 0 END) AS returned_qty,
    -sum(CASE WHEN movement_type = 'exchange_out' THEN quantity_delta ELSE 0 END) AS exchange_out_qty,
    sum(CASE WHEN movement_type = 'adjustment' THEN quantity_delta ELSE 0 END) AS adjustment_qty,
    sum(quantity_delta) AS net_qty
  FROM {{ source('silver', 'inventory_transactions') }}
  WHERE created_at <= {{ as_of() }}
  GROUP BY 1, 2, 3, 4
),
balances AS (
  SELECT
    m.*,
    sum(net_qty) OVER (PARTITION BY location_type, store_key, variant_key ORDER BY movement_date) AS closing_qty,
    coalesce(
      lead(movement_date) OVER (PARTITION BY location_type, store_key, variant_key ORDER BY movement_date),
      date_add('day', 1, {{ business_date() }})
    ) AS next_movement_date
  FROM movements m
)
SELECT
  day AS inventory_date,
  b.location_type,
  b.store_key,
  b.variant_key,
  v.product_id AS product_key,
  b.closing_qty - CASE WHEN day = b.movement_date THEN b.net_qty ELSE 0 END AS opening_qty,
  CASE WHEN day = b.movement_date THEN b.inbound_qty ELSE 0 END AS inbound_qty,
  CASE WHEN day = b.movement_date THEN b.sold_online_qty ELSE 0 END AS sold_online_qty,
  CASE WHEN day = b.movement_date THEN b.sold_pos_qty ELSE 0 END AS sold_pos_qty,
  CASE WHEN day = b.movement_date THEN b.transfer_out_qty ELSE 0 END AS transfer_out_qty,
  CASE WHEN day = b.movement_date THEN b.transfer_in_qty ELSE 0 END AS transfer_in_qty,
  CASE WHEN day = b.movement_date THEN b.returned_qty ELSE 0 END AS returned_qty,
  CASE WHEN day = b.movement_date THEN b.exchange_out_qty ELSE 0 END AS exchange_out_qty,
  CASE WHEN day = b.movement_date THEN b.adjustment_qty ELSE 0 END AS adjustment_qty,
  b.closing_qty,
  b.closing_qty * v.cost_price_vnd AS closing_value_vnd
FROM balances b
CROSS JOIN UNNEST(sequence(b.movement_date, date_add('day', -1, b.next_movement_date))) AS calendar(day)
LEFT JOIN {{ source('silver', 'product_variants') }} v ON v.variant_id = b.variant_key
