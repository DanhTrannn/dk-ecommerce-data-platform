{{ config(
    materialized='incremental',
    unique_key=['position_key', 'inventory_date'],
    properties={"partitioning": "ARRAY['month(inventory_date)']"}
) }}
-- One row per day for each stock position (central warehouse or store x variant), from its first movement up
-- to the last built day. Built from the stock ledger only: a position's balance is the running sum of its
-- movements. An incremental run extends every position to the new last day and rebuilds a position from its
-- earliest new movement, or from its first day when its variant's cost changed (stock is valued at current cost).
WITH
{% if is_incremental() %}
built AS (
  SELECT max(inventory_date) AS last_day FROM {{ this }}
),
bounds AS (
  SELECT greatest({{ business_date() }}, coalesce((SELECT last_day FROM built), {{ business_date() }})) AS last_day
),
changed AS (
  SELECT location_type, store_id AS store_key, variant_id AS variant_key, min({{ vn_date('created_at') }}) AS start_day
  FROM {{ ref('inventory_transactions') }}
  WHERE _ingested_at > {{ watermark() }}
  GROUP BY 1, 2, 3
  UNION ALL
  SELECT location_type, store_key, variant_key, min(inventory_date)
  FROM {{ this }}
  WHERE variant_key IN (SELECT variant_key FROM {{ ref('dim_variant') }} WHERE _ingested_at > {{ watermark() }})
  GROUP BY 1, 2, 3
  UNION ALL
  SELECT location_type, store_key, variant_key, date_add('day', 1, (SELECT last_day FROM built))
  FROM {{ this }}
  WHERE inventory_date = (SELECT last_day FROM built)
),
starts AS (
  SELECT
    s.location_type,
    s.store_key,
    s.variant_key,
    s.start_day,
    coalesce(t.closing_qty, 0) AS opening_qty
  FROM (SELECT location_type, store_key, variant_key, min(start_day) AS start_day FROM changed GROUP BY 1, 2, 3) s
  LEFT JOIN {{ this }} t
    ON t.location_type = s.location_type
    AND t.store_key IS NOT DISTINCT FROM s.store_key
    AND t.variant_key = s.variant_key
    AND t.inventory_date = date_add('day', -1, s.start_day)
  WHERE s.start_day <= (SELECT last_day FROM bounds)
),
{% else %}
bounds AS (
  SELECT {{ business_date() }} AS last_day
),
{% endif %}
ledger AS (
  SELECT t.*, {{ vn_date('t.created_at') }} AS movement_date
  FROM {{ ref('inventory_transactions') }} t
  {% if is_incremental() %}
  JOIN starts s
    ON s.location_type = t.location_type
    AND s.store_key IS NOT DISTINCT FROM t.store_id
    AND s.variant_key = t.variant_id
    AND {{ vn_date('t.created_at') }} >= s.start_day
  {% endif %}
  WHERE {{ vn_date('t.created_at') }} <= (SELECT last_day FROM bounds)
),
movements AS (
  SELECT
    movement_date,
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
    sum(quantity_delta) AS net_qty,
    max(_ingested_at) AS _ingested_at
  FROM ledger
  GROUP BY 1, 2, 3, 4
  {% if is_incremental() %}
  -- A restarted position needs a row on its start day even without a movement that day.
  UNION ALL
  SELECT start_day, location_type, store_key, variant_key, 0, 0, 0, 0, 0, 0, 0, 0, 0, CAST(NULL AS timestamp(6) with time zone)
  FROM starts
  {% endif %}
),
days AS (
  SELECT
    movement_date,
    location_type,
    store_key,
    variant_key,
    sum(inbound_qty) AS inbound_qty,
    sum(sold_online_qty) AS sold_online_qty,
    sum(sold_pos_qty) AS sold_pos_qty,
    sum(transfer_out_qty) AS transfer_out_qty,
    sum(transfer_in_qty) AS transfer_in_qty,
    sum(returned_qty) AS returned_qty,
    sum(exchange_out_qty) AS exchange_out_qty,
    sum(adjustment_qty) AS adjustment_qty,
    sum(net_qty) AS net_qty,
    max(_ingested_at) AS _ingested_at
  FROM movements
  GROUP BY 1, 2, 3, 4
),
balances AS (
  SELECT
    d.*,
    {% if is_incremental() %}s.opening_qty{% else %}0{% endif %}
      + sum(d.net_qty) OVER (PARTITION BY d.location_type, d.store_key, d.variant_key ORDER BY d.movement_date) AS closing_qty,
    max(d._ingested_at) OVER (PARTITION BY d.location_type, d.store_key, d.variant_key) AS position_ingested_at,
    coalesce(
      lead(d.movement_date) OVER (PARTITION BY d.location_type, d.store_key, d.variant_key ORDER BY d.movement_date),
      date_add('day', 1, (SELECT last_day FROM bounds))
    ) AS next_movement_date
  FROM days d
  {% if is_incremental() %}
  JOIN starts s
    ON s.location_type = d.location_type AND s.store_key IS NOT DISTINCT FROM d.store_key AND s.variant_key = d.variant_key
  {% endif %}
)
SELECT
  b.location_type || ':' || coalesce(CAST(b.store_key AS varchar), '') || ':' || CAST(b.variant_key AS varchar) AS position_key,
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
  b.closing_qty * v.cost_price_vnd AS closing_value_vnd,
  {{ latest('b.position_ingested_at', 'v._ingested_at') }} AS _ingested_at
FROM balances b
CROSS JOIN UNNEST(sequence(b.movement_date, date_add('day', -1, b.next_movement_date))) AS calendar(day)
LEFT JOIN {{ ref('product_variants') }} v ON v.variant_id = b.variant_key
