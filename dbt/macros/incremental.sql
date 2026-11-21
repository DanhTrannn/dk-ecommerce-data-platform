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

{#- The row's dates before this merge, read from the `prev` join on {{ this }}; marts recompute those days too. -#}
{% macro previous_dates() -%}
  {% if is_incremental() %}filter(ARRAY[{{ varargs | join(', ') }}], day -> day IS NOT NULL){% else %}CAST(ARRAY[] AS array(date)){% endif %}
{%- endmacro %}

{#- The Airflow run that merged a Gold row; marts recompute what the current run changed. Set DBT_RUN_ID when
    running dbt by hand, so the facts and the marts of one load share it. -#}
{% macro gold_run() -%}
  '{{ env_var("DBT_RUN_ID", invocation_id) }}'
{%- endmacro %}
