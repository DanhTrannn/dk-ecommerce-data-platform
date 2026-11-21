{% materialization iceberg_merge, adapter='trino' %}
  {# Tables are pre-created with the reviewed Iceberg schema. Never drop, rename or replace their identity. #}
  {% set columns = adapter.get_columns_in_relation(this) %}
  {% if not columns %}
    {{ exceptions.raise_compiler_error('Pre-create the Iceberg contract before running dbt: ' ~ this) }}
  {% endif %}
  {% set business = [] %}
  {% for column in columns if not column.name.startswith('_') %}
    {% do business.append(column) %}
  {% endfor %}
  {% call statement('main') %}
    MERGE INTO {{ this }} AS t
    USING (
      WITH expected AS ({{ sql }})
      SELECT {% for column in columns %}e.{{ adapter.quote(column.name) }}, {% endfor %}false AS _delete_row FROM expected e
      UNION ALL
      SELECT old.*, true AS _delete_row FROM {{ this }} old
      WHERE NOT EXISTS (
        SELECT 1 FROM expected e WHERE
        {% for column in business %}
          old.{{ adapter.quote(column.name) }} IS NOT DISTINCT FROM e.{{ adapter.quote(column.name) }}{% if not loop.last %} AND {% endif %}
        {% endfor %}
      )
    ) AS s ON
      {% for column in business %}
        t.{{ adapter.quote(column.name) }} IS NOT DISTINCT FROM s.{{ adapter.quote(column.name) }}{% if not loop.last %} AND {% endif %}
      {% endfor %}
    WHEN MATCHED AND s._delete_row THEN DELETE
    WHEN MATCHED THEN UPDATE SET
      {% for column in columns %}
        {{ adapter.quote(column.name) }} = s.{{ adapter.quote(column.name) }}{% if not loop.last %}, {% endif %}
      {% endfor %}
    WHEN NOT MATCHED AND NOT s._delete_row THEN INSERT (
      {% for column in columns %}{{ adapter.quote(column.name) }}{% if not loop.last %}, {% endif %}{% endfor %}
    ) VALUES (
      {% for column in columns %}s.{{ adapter.quote(column.name) }}{% if not loop.last %}, {% endif %}{% endfor %}
    )
  {% endcall %}
  {{ return({'relations': [this]}) }}
{% endmaterialization %}
