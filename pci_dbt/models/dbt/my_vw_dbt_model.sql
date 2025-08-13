
-- Use the `ref` function to select from other models

select *
from {{ ref('my_tbl_dbt_model') }}
where id = 1
