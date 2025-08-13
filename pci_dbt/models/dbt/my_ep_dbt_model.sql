
-- Use the `ref` function to select from other models

--{{ config(materialized='ephemeral',unique_key= 'id') }}
{{ config(materialized='view') }}
select *
from {{ ref('my_tbl_dbt_model') }}
where id = 1
