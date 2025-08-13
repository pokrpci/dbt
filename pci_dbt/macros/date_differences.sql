{% macro calculate_date_differences(table_name, date_column) %}
    select
        {{ date_column }} as usr_CreatedDate, CURRENT_TIMESTAMP as [current_date],
        DATEDIFF(YY, {{ date_column }}, GETDATE()) as total_time_years,
        DATEDIFF(MM, {{ date_column }}, GETDATE()) as total_time_months,
        DATEDIFF(DD, {{ date_column }}, GETDATE()) as total_time_days
    from {{ table_name }}
{% endmacro %}
