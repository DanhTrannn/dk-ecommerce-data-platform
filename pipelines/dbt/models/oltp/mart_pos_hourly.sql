-- Recognized POS orders by local hour of sale and the staff member who sold them.
SELECT
  o.order_date,
  {{ vn_hour('o.created_at') }} AS order_hour,
  o.store_key,
  s.store_name,
  o.sales_staff_key,
  st.staff_name AS sales_staff_name,
  count(*) AS orders,
  sum(o.subtotal_vnd - o.discount_vnd) AS revenue_vnd,
  sum(o.item_quantity) AS items_sold
FROM {{ ref('fact_order') }} o
LEFT JOIN {{ ref('dim_store') }} s ON s.store_key = o.store_key
LEFT JOIN {{ ref('dim_staff') }} st ON st.staff_key = o.sales_staff_key
WHERE o.channel = 'pos' AND o.revenue_date IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6
