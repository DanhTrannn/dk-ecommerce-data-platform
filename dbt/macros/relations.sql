{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ (custom_schema_name or 'gold') | trim }}
{%- endmacro %}

{% macro business_date() -%}
  CAST(at_timezone(from_iso8601_timestamp_nanos('{{ var("as_of", env_var("DBT_AS_OF", "1970-01-01T00:00:00Z")) }}'), 'Asia/Ho_Chi_Minh') AS date)
{%- endmacro %}

{% macro floor_allocate(amount, running, total) -%}
  CAST((CAST({{ amount }} AS decimal(38,0)) * CAST({{ running }} AS decimal(38,0))
    - mod(CAST({{ amount }} AS decimal(38,0)) * CAST({{ running }} AS decimal(38,0)),
          greatest(CAST({{ total }} AS decimal(38,0)), decimal '1')))
    / greatest(CAST({{ total }} AS decimal(38,0)), decimal '1') AS bigint)
{%- endmacro %}

{% macro as_of() -%}
  from_iso8601_timestamp('{{ var("as_of", env_var("DBT_AS_OF", "1970-01-01T00:00:00Z")) }}')
{%- endmacro %}

{% macro vn_date(column) -%}
  CAST(at_timezone({{ column }}, 'Asia/Ho_Chi_Minh') AS date)
{%- endmacro %}

{% macro vn_hour(column) -%}
  hour(at_timezone({{ column }}, 'Asia/Ho_Chi_Minh'))
{%- endmacro %}
