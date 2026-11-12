{{ config(tags=["oltp"]) }}
WITH orders AS (SELECT sum(net_revenue_vnd) value FROM {{ ref('fact_order') }}),
items AS (SELECT sum(net_revenue_vnd) value FROM {{ ref('fact_order_item') }}),
mart AS (SELECT sum(net_revenue_vnd) value FROM {{ ref('mart_order_daily') }})
SELECT orders.value orders_value,items.value items_value,mart.value mart_value
FROM orders CROSS JOIN items CROSS JOIN mart
WHERE coalesce(orders.value,0) <> coalesce(items.value,0) OR coalesce(orders.value,0) <> coalesce(mart.value,0)
