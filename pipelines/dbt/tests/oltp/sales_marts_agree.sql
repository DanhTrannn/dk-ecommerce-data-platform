-- Revenue after discount is the same in the line fact, mart_sales_daily and mart_product_sales_daily;
-- mart refunds equal the succeeded refunds of recognized orders.
WITH lines AS (SELECT sum(revenue_vnd) AS revenue FROM {{ ref('fact_order_item') }} WHERE revenue_date IS NOT NULL),
sales AS (SELECT sum(gross_revenue_vnd - discount_vnd) AS revenue, sum(refund_vnd) AS refunds FROM {{ ref('mart_sales_daily') }}),
products AS (SELECT sum(revenue_vnd) AS revenue FROM {{ ref('mart_product_sales_daily') }}),
refunds AS (SELECT sum(refunded_vnd) AS refunds FROM {{ ref('fact_order') }} WHERE revenue_date IS NOT NULL)
SELECT * FROM lines, sales, products, refunds
WHERE lines.revenue IS DISTINCT FROM sales.revenue
   OR lines.revenue IS DISTINCT FROM products.revenue
   OR coalesce(sales.refunds, 0) <> coalesce(refunds.refunds, 0)
