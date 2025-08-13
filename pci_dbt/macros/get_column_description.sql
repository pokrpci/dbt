{% macro get_column_description(source_model, column_name) %}
 {% set source_columns = model.meta[source_model]['columns'] %}
 {% if column_name in source_columns %}
   {{ source_columns[column_name]['description'] }}
 {% else %}
    {{ "No description available" }}
 {% endif %}
{% endmacro %}
