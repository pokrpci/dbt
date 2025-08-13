-- analyses/survey_questions.sql
with source_data as (
    select * from {{ source('dbinterface', 'gc_SURVEY_QUESTIONS') }}
)
select
   id,
   question_id,
   [TEXT],
    count(*) as record_count
from
    source_data
group by
    survey_id, question_id