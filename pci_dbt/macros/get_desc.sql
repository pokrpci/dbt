{% macro get_desc(source_name, table_name, column_name) %}
  {% set source_table = source(source_name, table_name) %}
  {% set column = source_table.columns | selectattr('name', 'equalto', column_name) | first %}

  {{ log('source_name: ' ~ source_name ~ ', table_name: ' ~ table_name ~ ', column_name: ' ~ column_name, info=True) }}
  {{ log('get_desc macro called', info=True) }}
  {{ log("Source table: " ~ source_table, info=True) }}
  {{ log("Column: " ~ column, info=True) }}

  {% if column %}
    {{ column.description }}
  {% else %}
    {{ log("Column " ~ column_name ~ " not found in " ~ table_name, info=True) }}
    {{ return("Description not found") }}
  {% endif %}
{% endmacro %}
