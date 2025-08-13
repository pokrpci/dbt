-- macros/load_to_sqlserver.sql
{% macro load_to_sqlserver() %}
    {% set sqlserver_table = 'DBT.dbo.snow_UserLogin' %}
    {% set snowflake_table = ref('USER_LOGIN') %}

    INSERT INTO {{ sqlserver_table }}
    SELECT * FROM {{ snowflake_table }}
{% endmacro %};
