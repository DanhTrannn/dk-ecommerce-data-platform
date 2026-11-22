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

{#- Created by the lakehouse_oltp_init DAG. Marts only append to it, since marts running in parallel that update
    shared rows conflict on commit; a mart's progress is its latest row. -#}
{% macro mart_state() -%}
  {{ api.Relation.create(database=target.database, schema='gold', identifier='_mart_state') }}
{%- endmacro %}

{#- Run `sql`, which returns one varchar, and inline the result as a literal, or NULL when it returns NULL and there
    is no default. Trino repeats a subquery wherever a CTE uses it, and a literal also lets Iceberg skip files by
    their column statistics. -#}
{% macro literal(sql, type, default=none) -%}
  {%- set value = run_query(sql).columns[0].values()[0] if execute else none -%}
  {%- set value = value if value is not none else default -%}
  {%- if value is none -%}CAST(NULL AS {{ type }}){%- else -%}{{ type }} '{{ value }}'{%- endif -%}
{%- endmacro %}

{#- The latest Gold `_ingested_at` this mart has built from; Gold rows ingested later are new to it. The pre-hook
    and the model read it before the post-hook appends a new row, so both see the same value. -#}
{% macro mart_watermark() -%}
  {{ literal("SELECT CAST(max(watermark) AS varchar) FROM " ~ mart_state() ~ " WHERE mart = '" ~ this.name ~ "'",
             'TIMESTAMP', '1970-01-01 00:00:00 UTC') }}
{%- endmacro %}

{#- The last day this mart has built, or NULL before its first build. -#}
{% macro mart_last_day() -%}
  {{ literal("SELECT CAST(max(last_day) AS varchar) FROM " ~ mart_state() ~ " WHERE mart = '" ~ this.name ~ "'", 'DATE') }}
{%- endmacro %}

{#- Post-hook: record that the mart now covers every row of `inputs`, and `last_day` when it builds a day series.
    It runs only after the mart was written, so a failed run leaves the old watermark for the next attempt. -#}
{% macro advance_mart(inputs, last_day='CAST(NULL AS date)') -%}
  INSERT INTO {{ mart_state() }} (mart, watermark, last_day)
  SELECT
    '{{ this.name }}',
    greatest({% for input in inputs %}(SELECT max(_ingested_at) FROM {{ ref(input) }}){{ ", " if not loop.last }}{% endfor %}),
    {{ last_day }}
{%- endmacro %}

{#- The dates the row had before this merge, read from the `prev` join on this model. Older dates are kept until
    every mart has built past the previous merge, so a mart that failed still recomputes them. -#}
{% macro previous_dates() -%}
  {% if is_incremental() %}
  array_distinct(filter(
    concat(
      ARRAY[{{ varargs | join(', ') }}],
      CASE
        WHEN prev._ingested_at > {{ literal("SELECT CAST(min(watermark) AS varchar) FROM (SELECT max(watermark) AS watermark FROM "
                                            ~ mart_state() ~ " GROUP BY mart)", 'TIMESTAMP', '1970-01-01 00:00:00 UTC') }}
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

{#- The rows of `model` still present at the source. Marts read through it; the macros that pick the days to
    recompute read the deleted rows too. -#}
{% macro live(model) -%}
  (SELECT * FROM {{ ref(model) }} WHERE NOT _is_deleted)
{%- endmacro %}
