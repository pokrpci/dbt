{% macro get_columns_from_source(source_name, table_name) %}
  {% set source_relation = source(source_name, table_name) %}
  {% set columns = adapter.get_columns_in_relation(source_relation) %}
  {{ columns | join(', ') }}
{% endmacro %}

-- macros/get_column_description.sql
-- This macro retrieves the description of a column from the source model's metadata.
-- It checks if the column exists in the source model's columns and returns its description.