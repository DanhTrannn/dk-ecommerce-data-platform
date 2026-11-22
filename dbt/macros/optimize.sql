{#- Rewrite the small files of every OLTP Silver and Gold table and of the mart state; Trino skips tables that
    need no rewrite. -#}
{% macro optimize() %}
  {% for node in graph.nodes.values()
       if node.resource_type in ('model', 'seed') and ('oltp' in node.tags or 'silver' in node.tags) %}
    {% set relation = api.Relation.create(database=node.database, schema=node.schema, identifier=node.alias) %}
    {% do run_query('ALTER TABLE ' ~ relation ~ ' EXECUTE optimize') %}
    {% do log('OPTIMIZE ' ~ relation, info=True) %}
  {% endfor %}
  {% do run_query('ALTER TABLE ' ~ mart_state() ~ ' EXECUTE optimize') %}
{% endmacro %}
