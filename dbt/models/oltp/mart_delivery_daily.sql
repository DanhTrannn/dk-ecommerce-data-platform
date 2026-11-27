-- Shipments that ended (delivered or failed) per day; durations are sums with their counts so averages stay
-- correct at any roll-up.
SELECT
  s.end_date,
  s.city_key,
  c.city_name,
  s.delivery_staff_key,
  st.staff_name AS delivery_staff_name,
  s.payment_method,
  count(*) AS shipments,
  count_if(s.delivered_at IS NOT NULL) AS delivered_shipments,
  count_if(s.delivered_at IS NULL) AS failed_shipments,
  count_if(s.attempt_count > 1) AS redelivered_shipments,
  count_if(s.is_on_time) AS on_time_shipments,
  sum(CASE WHEN s.delivered_at IS NOT NULL THEN date_diff('second', s.order_created_at, s.delivered_at) END) / 3600e0 AS order_to_delivery_hours,
  sum(CASE WHEN s.delivered_at IS NOT NULL AND s.dispatched_at IS NOT NULL THEN date_diff('second', s.dispatched_at, s.delivered_at) END) / 3600e0 AS dispatch_to_delivery_hours,
  count_if(s.delivered_at IS NOT NULL AND s.dispatched_at IS NOT NULL) AS dispatched_and_delivered_shipments,
  sum(s.cod_amount_vnd) AS cod_due_vnd,
  sum(s.cod_collected_vnd) AS cod_collected_vnd
FROM {{ live('fact_shipment') }} s
LEFT JOIN {{ live('dim_city') }} c ON c.city_key = s.city_key
LEFT JOIN {{ live('dim_staff') }} st ON st.staff_key = s.delivery_staff_key
WHERE s.end_date IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6
