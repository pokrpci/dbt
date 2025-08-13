{{ config(materialized='ephemeral') }}

SELECT [ID]
      ,[QUESTION_ID]
      ,[QUESTION_GROUP_ID]
      ,[CREATE_AUDIT_KEY]
      ,[UPDATE_AUDIT_KEY]
      ,[TEXT]
      ,[HELP_TEXT]
      ,[TYPE]
      ,[NA_ENABLED]
      ,[TRACKING_HASH]
      ,[LAST_MODIFIED]
      ,[ZLOADDATE]
 FROM {{ source('dbinterface', 'gc_SURVEY_QUESTIONS') }}
WHERE 1=1