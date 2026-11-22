{#- Marts recompute whole days (or customers): those of the Gold rows ingested after the mart's watermark. A
    pre-hook deletes them and the model inserts them again, which also empties a day whose last row moved to
    another day. Both read the same Gold rows and the same watermark, which only the post-hook advances, so a retry
    or the next run recomputes the same values again. -#}

{% macro delete_in(column, values) -%}
  {% if is_incremental() %}DELETE FROM {{ this }} WHERE {{ column }} IN ({{ values }}){% endif %}
{%- endmacro %}

{% macro filter_in(column, values) -%}
  {% if is_incremental() %}{{ column }} IN ({{ values }}){% else %}true{% endif %}
{%- endmacro %}

{#- The current and previous dates of the rows of `model` ingested after the mart's watermark. -#}
{% macro changed_days(model, columns) -%}
  SELECT day
  FROM {{ ref(model) }}
  CROSS JOIN UNNEST(concat(ARRAY[{{ columns | join(', ') }}], _previous_dates)) AS t(day)
  WHERE _ingested_at > {{ mart_watermark() }} AND day IS NOT NULL
{%- endmacro %}

{#- The dates of the rows of `model` whose dimension changed; the mart shows the dimension's names. -#}
{% macro dimension_days(model, column, key, dimension, dimension_key) -%}
  SELECT {{ column }}
  FROM {{ ref(model) }}
  WHERE {{ column }} IS NOT NULL
    AND {{ key }} IN (SELECT {{ dimension_key }} FROM {{ ref(dimension) }} WHERE _ingested_at > {{ mart_watermark() }})
{%- endmacro %}

{% macro order_days() -%}
  {{ changed_days('fact_order', ['order_date']) }}
  UNION {{ dimension_days('fact_order', 'order_date', 'store_key', 'dim_store', 'store_key') }}
  UNION {{ dimension_days('fact_order', 'order_date', 'city_key', 'dim_city', 'city_key') }}
  UNION {{ dimension_days('fact_order', 'order_date', 'sales_staff_key', 'dim_staff', 'staff_key') }}
{%- endmacro %}

{% macro review_days() -%}
  {{ changed_days('fact_review', ['review_date']) }}
  UNION {{ dimension_days('fact_review', 'review_date', 'product_key', 'dim_product', 'product_key') }}
{%- endmacro %}

{% macro return_days() -%}
  {{ changed_days('fact_return', ['request_date']) }}
  UNION {{ dimension_days('fact_return', 'request_date', 'variant_key', 'dim_variant', 'variant_key') }}
  UNION {{ dimension_days('fact_return', 'request_date', 'product_key', 'dim_product', 'product_key') }}
{%- endmacro %}

{% macro delivery_days() -%}
  {{ changed_days('fact_shipment', ['end_date']) }}
  UNION {{ dimension_days('fact_shipment', 'end_date', 'city_key', 'dim_city', 'city_key') }}
  UNION {{ dimension_days('fact_shipment', 'end_date', 'delivery_staff_key', 'dim_staff', 'staff_key') }}
{%- endmacro %}

{#- Revenue, refund and return-cost days of every touched order: an order recognized later also moves its earlier
    refunds into the revenue marts. -#}
{% macro sales_days() -%}
  WITH orders AS (
    SELECT order_id FROM {{ ref('fact_order') }} WHERE _ingested_at > {{ mart_watermark() }}
    UNION SELECT order_id FROM {{ ref('fact_order_item') }} WHERE _ingested_at > {{ mart_watermark() }}
    UNION SELECT order_id FROM {{ ref('fact_return') }} WHERE _ingested_at > {{ mart_watermark() }}
    UNION
    SELECT order_id FROM {{ ref('fact_order') }}
    WHERE store_key IN (SELECT store_key FROM {{ ref('dim_store') }} WHERE _ingested_at > {{ mart_watermark() }})
      OR city_key IN (SELECT city_key FROM {{ ref('dim_city') }} WHERE _ingested_at > {{ mart_watermark() }})
    UNION
    SELECT order_id FROM {{ ref('fact_order_item') }}
    WHERE product_key IN (SELECT product_key FROM {{ ref('dim_product') }} WHERE _ingested_at > {{ mart_watermark() }})
  )
  SELECT revenue_date FROM {{ ref('fact_order') }} WHERE order_id IN (SELECT order_id FROM orders) AND revenue_date IS NOT NULL
  UNION
  SELECT {{ vn_date('r.completed_at') }}
  FROM {{ ref('refunds') }} r
  JOIN {{ ref('payments') }} p ON p.payment_id = r.payment_id
  WHERE p.order_id IN (SELECT order_id FROM orders) AND r.completed_at IS NOT NULL
  UNION
  SELECT day
  FROM {{ ref('fact_return') }}
  CROSS JOIN UNNEST(ARRAY[{{ vn_date('refunded_at') }}, resolved_date]) AS t(day)
  WHERE order_id IN (SELECT order_id FROM orders) AND day IS NOT NULL
  UNION {{ changed_days('fact_order', ['revenue_date']) }}
  UNION {{ changed_days('fact_order_item', ['revenue_date']) }}
  UNION {{ changed_days('fact_return', [vn_date('refunded_at'), 'resolved_date']) }}
{%- endmacro %}

{% macro customer_keys() -%}
  SELECT customer_key FROM {{ ref('fact_order') }} WHERE _ingested_at > {{ mart_watermark() }} AND customer_key IS NOT NULL
  UNION SELECT customer_key FROM {{ ref('dim_customer') }} WHERE _ingested_at > {{ mart_watermark() }}
{%- endmacro %}

{#- Days with a changed stock row, plus the days fact_inventory_daily added after the mart's last day. -#}
{% macro inventory_days() -%}
  SELECT inventory_date FROM {{ ref('fact_inventory_daily') }}
  WHERE _ingested_at > {{ mart_watermark() }}
    OR inventory_date > coalesce({{ mart_last_day() }}, DATE '1970-01-01')
  UNION {{ dimension_days('fact_inventory_daily', 'inventory_date', 'product_key', 'dim_product', 'product_key') }}
  UNION {{ dimension_days('fact_inventory_daily', 'inventory_date', 'store_key', 'dim_store', 'store_key') }}
{%- endmacro %}

{% macro inventory_last_day() -%}
  (SELECT max(inventory_date) FROM {{ ref('fact_inventory_daily') }})
{%- endmacro %}

{#- The last backlog day: the as-of day, or the newest order day when a backfill runs with an older as-of day. -#}
{% macro backlog_last_day() -%}
  {% if is_incremental() %}
  (SELECT greatest({{ business_date() }}, coalesce(max(order_date), {{ business_date() }})) FROM {{ ref('fact_order') }})
  {% else %}
  {{ business_date() }}
  {% endif %}
{%- endmacro %}

{#- From the earliest order day touched, or the first day after the mart's last day, to the last backlog day: an
    order's open days all fall on or after the day it was placed. -#}
{% macro backlog_days() -%}
  SELECT day
  FROM UNNEST(sequence(
    least(
      (
        SELECT coalesce(min(order_date), {{ backlog_last_day() }})
        FROM {{ ref('fact_order') }}
        WHERE channel = 'online'
          AND (_ingested_at > {{ mart_watermark() }}
            OR order_id IN (SELECT order_id FROM {{ ref('fact_shipment') }} WHERE _ingested_at > {{ mart_watermark() }}))
      ),
      coalesce(date_add('day', 1, {{ mart_last_day() }}), {{ backlog_last_day() }}),
      {{ backlog_last_day() }}
    ),
    {{ backlog_last_day() }}
  )) AS t(day)
{%- endmacro %}
