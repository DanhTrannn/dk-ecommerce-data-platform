-- Cohort = month of a customer's first recognized order; same order basis as mart_customer_value.
WITH orders AS (
  SELECT
    o.customer_key,
    date_trunc('month', o.revenue_date) AS revenue_month,
    o.subtotal_vnd - o.discount_vnd - o.refunded_vnd AS net_revenue_vnd
  FROM {{ ref('fact_order') }} o
  JOIN {{ ref('dim_customer') }} c ON c.customer_key = o.customer_key
  WHERE o.revenue_date IS NOT NULL
),
cohorts AS (
  SELECT customer_key, min(revenue_month) AS cohort_month
  FROM orders
  GROUP BY customer_key
)
SELECT
  c.cohort_month,
  date_diff('month', c.cohort_month, o.revenue_month) AS months_since_first_order,
  count(DISTINCT o.customer_key) AS active_customers,
  count(*) AS orders,
  sum(o.net_revenue_vnd) AS net_revenue_vnd
FROM orders o
JOIN cohorts c ON c.customer_key = o.customer_key
GROUP BY 1, 2
