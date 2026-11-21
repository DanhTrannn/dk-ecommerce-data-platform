SELECT inventory_date, location_type, store_key, variant_key
FROM {{ ref('fact_inventory_daily') }}
GROUP BY 1, 2, 3, 4
HAVING count(*) > 1
