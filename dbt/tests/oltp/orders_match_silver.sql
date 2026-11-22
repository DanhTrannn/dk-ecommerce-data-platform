-- fact_order and mart_orders_daily keep every live Silver order and its total.
WITH silver AS (SELECT count(*) AS orders, sum(total_vnd) AS total_vnd FROM {{ live('orders') }}),
fact AS (SELECT count(*) AS orders, sum(total_vnd) AS total_vnd FROM {{ live('fact_order') }}),
mart AS (SELECT sum(orders) AS orders, sum(order_value_vnd) AS total_vnd FROM {{ ref('mart_orders_daily') }})
SELECT * FROM silver, fact, mart
WHERE silver.orders <> fact.orders OR silver.orders <> mart.orders
   OR silver.total_vnd IS DISTINCT FROM fact.total_vnd OR silver.total_vnd IS DISTINCT FROM mart.total_vnd
