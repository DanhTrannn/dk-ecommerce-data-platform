SELECT
  CAST(b."order_item_id" AS bigint) AS "order_item_id",
  CAST(b."order_id" AS bigint) AS "order_id",
  CAST(b."order_date_key" AS integer) AS "order_date_key",
  CAST(b."order_date" AS date) AS "order_date",
  CAST(b."customer_key" AS bigint) AS "customer_key",
  CAST(b."product_key" AS bigint) AS "product_key",
  CAST(b."variant_key" AS bigint) AS "variant_key",
  CAST(b."store_key" AS bigint) AS "store_key",
  CAST(b."channel" AS varchar) AS "channel",
  CAST(b."order_status" AS varchar) AS "order_status",
  CAST(b."quantity" AS integer) AS "quantity",
  CAST(b."unit_price_vnd" AS bigint) AS "unit_price_vnd",
  CAST(b."unit_cost_vnd" AS bigint) AS "unit_cost_vnd",
  CAST(b."item_total_vnd" AS bigint) AS "item_total_vnd",
  CAST(b."item_cost_vnd" AS bigint) AS "item_cost_vnd",
  CAST(b."item_profit_vnd" AS bigint) AS "item_profit_vnd",
  CAST(b."category_code_snapshot" AS varchar) AS "category_code_snapshot",
  CAST(b."category_name_snapshot" AS varchar) AS "category_name_snapshot",
  CAST(b."allocated_discount_vnd" AS bigint) AS "allocated_discount_vnd",
  CAST(b."returned_quantity" AS integer) AS "returned_quantity",
  CAST(b."recognized_quantity" AS integer) AS "recognized_quantity",
  CAST(b."gross_revenue_vnd" AS bigint) AS "gross_revenue_vnd",
  CAST(b."refunded_amount_vnd" AS bigint) AS "refunded_amount_vnd",
  CAST(b."net_revenue_vnd" AS bigint) AS "net_revenue_vnd",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
WITH base AS (
  SELECT i.order_item_id,i.order_id,o.created_at,coalesce(o.customer_id,-1) customer_key,
    v.product_id product_key,i.variant_id variant_key,coalesce(o.store_id,0) store_key,o.channel,o.status order_status,
    i.quantity,i.unit_price_vnd,i.cost_price_vnd unit_cost_vnd,i.quantity*i.unit_price_vnd item_total_vnd,
    coalesce(o.discount_amount_vnd,0) _order_discount,i.category_code_snapshot,i.category_name_snapshot
  FROM {{ pinned('order_items') }} i JOIN {{ pinned('orders') }} o ON i.order_id=o.order_id LEFT JOIN {{ pinned('product_variants') }} v ON i.variant_id=v.variant_id
), discount_weights AS (
  SELECT b.*,sum(CAST(item_total_vnd AS decimal(38,0))) OVER (PARTITION BY order_id) _total,
    sum(CAST(item_total_vnd AS decimal(38,0))) OVER (PARTITION BY order_id ORDER BY order_item_id ROWS UNBOUNDED PRECEDING) _running
  FROM base b
), discounted AS (
  SELECT b.*,least(_order_discount,_total) _discount FROM discount_weights b
), allocation AS (
  SELECT b.*,{{ floor_allocate('_discount','_running','_total') }} - {{ floor_allocate('_discount','(_running-item_total_vnd)','_total') }} allocated_discount_vnd
  FROM discounted b
), merchandise AS (
  SELECT b.*,item_total_vnd-allocated_discount_vnd _merchandise FROM allocation b
), returned AS (
  SELECT i.order_item_id,sum(i.quantity) _returned,sum(i.refund_amount_vnd) _return_value
  FROM {{ pinned('return_items') }} i JOIN {{ pinned('return_requests') }} r ON i.return_id=r.return_id
  WHERE r.status='completed' AND i.inspection_status='passed' GROUP BY i.order_item_id
), refunds_by_order AS (
  SELECT p.order_id,sum(r.amount_vnd) amount FROM {{ pinned('refunds') }} r JOIN {{ pinned('payments') }} p ON r.payment_id=p.payment_id
  WHERE r.status='succeeded' GROUP BY p.order_id
), joined AS (
  SELECT b.*,least(quantity,coalesce(r._returned,0)) returned_quantity,
    least(_merchandise,coalesce(r._return_value,0)) _return_weight,
    least(coalesce(f.amount,0),sum(_merchandise) OVER (PARTITION BY b.order_id)) _refund_total
  FROM merchandise b LEFT JOIN returned r ON b.order_item_id=r.order_item_id LEFT JOIN refunds_by_order f ON b.order_id=f.order_id
), refund_weights AS (
  SELECT b.*,CASE WHEN sum(_return_weight) OVER (PARTITION BY order_id)>0 THEN _return_weight ELSE _merchandise END _refund_weight
  FROM joined b
), refund_running AS (
  SELECT b.*,sum(CAST(_refund_weight AS decimal(38,0))) OVER (PARTITION BY order_id) _refund_weight_total,
    sum(CAST(_refund_weight AS decimal(38,0))) OVER (PARTITION BY order_id ORDER BY order_item_id ROWS UNBOUNDED PRECEDING) _refund_running
  FROM refund_weights b
), targeted AS (
  SELECT b.*,least(_refund_total,_refund_weight_total) _targeted_refund FROM refund_running b
), targeted_amount AS (
  SELECT b.*,{{ floor_allocate('_targeted_refund','_refund_running','_refund_weight_total') }} - {{ floor_allocate('_targeted_refund','(_refund_running-_refund_weight)','_refund_weight_total') }} _targeted_amount
  FROM targeted b
), remaining AS (
  SELECT b.*,_refund_total-_targeted_refund _remaining_refund,_merchandise-_targeted_amount _remaining_value FROM targeted_amount b
), remaining_running AS (
  SELECT b.*,sum(CAST(_remaining_value AS decimal(38,0))) OVER (PARTITION BY order_id) _remaining_total,
    sum(CAST(_remaining_value AS decimal(38,0))) OVER (PARTITION BY order_id ORDER BY order_item_id ROWS UNBOUNDED PRECEDING) _remaining_running
  FROM remaining b
), remaining_amount AS (
  SELECT b.*,{{ floor_allocate('_remaining_refund','_remaining_running','_remaining_total') }} - {{ floor_allocate('_remaining_refund','(_remaining_running-_remaining_value)','_remaining_total') }} _other_refund
  FROM remaining_running b
), recognized AS (
  SELECT b.*,
    CASE WHEN order_status IN ('delivered','completed','returned') THEN quantity-returned_quantity ELSE 0 END recognized_quantity,
    CASE WHEN order_status IN ('delivered','completed','returned') THEN _merchandise ELSE 0 END gross_revenue_vnd,
    CASE WHEN order_status IN ('delivered','completed','returned') THEN _targeted_amount+_other_refund ELSE 0 END refunded_amount_vnd
  FROM remaining_amount b
)
SELECT b.*,CAST(at_timezone(created_at,'Asia/Ho_Chi_Minh') AS date) order_date,
  CAST(date_format(at_timezone(created_at,'Asia/Ho_Chi_Minh'),'%Y%m%d') AS integer) order_date_key,
  gross_revenue_vnd-refunded_amount_vnd net_revenue_vnd,
  recognized_quantity*unit_cost_vnd item_cost_vnd,
  gross_revenue_vnd-refunded_amount_vnd-recognized_quantity*unit_cost_vnd item_profit_vnd
FROM recognized b

) b
