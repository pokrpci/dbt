-- models/extract_from_snowflake.sql
{{ config(materialized='ephemeral') }}

SELECT
    *
FROM
    {{ source('snowflake', 'USER_LOGIN') }}