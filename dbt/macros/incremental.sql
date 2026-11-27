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

{#- The rows of `model` still present at the source; marts read through it. -#}
{% macro live(model) -%}
  (SELECT * FROM {{ ref(model) }} WHERE NOT _is_deleted)
{%- endmacro %}
