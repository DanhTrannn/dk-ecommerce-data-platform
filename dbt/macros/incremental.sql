{#- The latest input ingestion this model has merged; later Silver or Gold rows are new to it. -#}
{% macro watermark() -%}
  (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{%- endmacro %}

{#- Keep only the keys in the model's `changed` CTE on an incremental run; keep every row on a full build. -#}
{% macro in_changed(column) -%}
  {% if is_incremental() %}{{ column }} IN (SELECT key FROM changed){% else %}true{% endif %}
{%- endmacro %}

{#- The latest of several `_ingested_at` values; a missing joined row does not hide the others. -#}
{% macro latest() -%}
  greatest({% for column in varargs %}coalesce({{ column }}, TIMESTAMP '1970-01-01 00:00:00 UTC'){{ ", " if not loop.last }}{% endfor %})
{%- endmacro %}

{#- Created by the lakehouse_oltp_init DAG; one row per mart. -#}
{% macro mart_state() -%}
  {{ api.Relation.create(database=target.database, schema='gold', identifier='_mart_state') }}
{%- endmacro %}

{#- The latest Gold `_ingested_at` this mart has built from; Gold rows ingested later are new to it. -#}
{% macro mart_watermark() -%}
  (SELECT coalesce(max(watermark), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ mart_state() }} WHERE mart = '{{ this.name }}')
{%- endmacro %}

{#- The last day this mart has built, or NULL before its first build. -#}
{% macro mart_last_day() -%}
  (SELECT max(last_day) FROM {{ mart_state() }} WHERE mart = '{{ this.name }}')
{%- endmacro %}

{#- Post-hook: record that the mart now covers every row of `inputs`, and `last_day` when it builds a day series.
    It runs only after the mart was written, so a failed run leaves the old watermark for the next attempt. -#}
{% macro advance_mart(inputs, last_day='CAST(NULL AS date)') -%}
  MERGE INTO {{ mart_state() }} s
  USING (
    SELECT
      '{{ this.name }}' AS mart,
      greatest({% for input in inputs %}(SELECT max(_ingested_at) FROM {{ ref(input) }}){{ ", " if not loop.last }}{% endfor %}) AS watermark,
      {{ last_day }} AS last_day
  ) n
  ON s.mart = n.mart
  WHEN MATCHED THEN UPDATE SET watermark = n.watermark, last_day = n.last_day
  WHEN NOT MATCHED THEN INSERT (mart, watermark, last_day) VALUES (n.mart, n.watermark, n.last_day)
{%- endmacro %}

{#- The dates the row had before this merge, read from the `prev` join on this model. Older dates are kept until
    every mart has built past the previous merge, so a mart that failed still recomputes them. -#}
{% macro previous_dates() -%}
  {% if is_incremental() %}
  array_distinct(filter(
    concat(
      ARRAY[{{ varargs | join(', ') }}],
      CASE
        WHEN prev._ingested_at > (SELECT coalesce(min(watermark), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ mart_state() }})
        THEN prev._previous_dates
        ELSE CAST(ARRAY[] AS array(date))
      END
    ),
    day -> day IS NOT NULL
  ))
  {% else %}
  CAST(ARRAY[] AS array(date))
  {% endif %}
{%- endmacro %}
