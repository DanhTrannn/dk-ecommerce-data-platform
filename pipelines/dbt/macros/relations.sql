{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ env_var('DBT_GOLD_SCHEMA', 'gold') }}
{%- endmacro %}

{% macro pinned(name) -%}
  {% set relation = source('events' if name == 'fact_web_events' else 'silver', name) %}
  {% if execute and name not in var('inputs', {}) %}{{ exceptions.raise_compiler_error('Use the pipeline wrapper to freeze input snapshots') }}{% endif %}
  {% set snapshot = var('inputs', {}).get(name) %}
  {% if snapshot is none %}
    (SELECT * FROM {{ relation }} WHERE false)
  {% else %}
    {{ relation }} FOR VERSION AS OF {{ snapshot }}
  {% endif %}
{%- endmacro %}

{% macro clock() -%}
  (SELECT CAST(from_iso8601_timestamp_nanos('{{ var("as_of", "1970-01-01T00:00:00Z") }}') AS timestamp(6) with time zone) AS observed_at)
{%- endmacro %}

{% macro business_date() -%}
  CAST(at_timezone(from_iso8601_timestamp_nanos('{{ var("as_of", "1970-01-01T00:00:00Z") }}'), 'Asia/Ho_Chi_Minh') AS date)
{%- endmacro %}

{% macro floor_allocate(amount, running, total) -%}
  CAST((CAST({{ amount }} AS decimal(38,0)) * CAST({{ running }} AS decimal(38,0))
    - mod(CAST({{ amount }} AS decimal(38,0)) * CAST({{ running }} AS decimal(38,0)),
          greatest(CAST({{ total }} AS decimal(38,0)), decimal '1')))
    / greatest(CAST({{ total }} AS decimal(38,0)), decimal '1') AS bigint)
{%- endmacro %}
