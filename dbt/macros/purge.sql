{#- Delete for good the rows flagged as deleted in Gold dimensions and facts, then in Silver. Gold goes first, so
    no Gold row outlives the Silver row it came from. -#}
{% macro purge_deleted() %}
  {% for tag in ('oltp', 'silver') %}
    {% for node in graph.nodes.values()
         if node.resource_type == 'model' and tag in node.tags
           and (tag == 'silver' or node.name.startswith(('dim_', 'fact_'))) and node.name != 'dim_date' %}
      {% set relation = api.Relation.create(database=node.database, schema=node.schema, identifier=node.alias) %}
      {% do run_query('DELETE FROM ' ~ relation ~ ' WHERE _is_deleted') %}
      {% do log('PURGE ' ~ relation, info=True) %}
    {% endfor %}
  {% endfor %}
{% endmacro %}
