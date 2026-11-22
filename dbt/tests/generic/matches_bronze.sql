{#- A Silver table holds the same keys as its Bronze table after every load, full or incremental.
    Returns the keys found in only one of them; both are keyed on `key`, so equal key sets mean equal row counts. -#}
{% test matches_bronze(model, key, bronze) %}
WITH bronze AS (
  SELECT {{ key }} AS key FROM {{ bronze }}
),
silver AS (
  SELECT {{ key }} AS key FROM {{ model }}
)
SELECT
  coalesce(bronze.key, silver.key) AS key,
  CASE WHEN silver.key IS NULL THEN 'missing_in_silver' ELSE 'missing_in_bronze' END AS problem
FROM bronze
FULL OUTER JOIN silver ON silver.key = bronze.key
WHERE bronze.key IS NULL OR silver.key IS NULL
{% endtest %}
