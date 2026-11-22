{{ config(materialized='incremental', incremental_strategy='append', pre_hook="{{ delete_in('review_date', review_days()) }}", post_hook="{{ advance_mart(['fact_review', 'dim_product']) }}") }}
SELECT
  r.review_date,
  r.product_key,
  p.product_name,
  r.delivery_timeliness,
  count(*) AS reviews,
  avg(r.rating) AS avg_rating,
  count_if(r.rating = 1) AS rating_1,
  count_if(r.rating = 2) AS rating_2,
  count_if(r.rating = 3) AS rating_3,
  count_if(r.rating = 4) AS rating_4,
  count_if(r.rating = 5) AS rating_5,
  count_if(r.status = 'rejected') AS rejected_reviews
FROM {{ live('fact_review') }} r
LEFT JOIN {{ live('dim_product') }} p ON p.product_key = r.product_key
WHERE {{ filter_in('r.review_date', review_days()) }}
GROUP BY 1, 2, 3, 4
