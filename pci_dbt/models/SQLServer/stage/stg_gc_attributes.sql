{{ config(materialized='ephemeral') }}

SELECT [ID]
      ,[CONVERSATION_ID]
      ,[START_DATE_TIME_KEY]
      ,[CREATE_AUDIT_KEY]
      ,[UPDATE_AUDIT_KEY]
      ,[PARTICIPANT_ID]
      ,[PARTICIPANT_PURPOSE]
      ,[KEYNAME]
      ,[VALUE]
      ,[LAST_MODIFIED]
      ,[ZLOADDATE]
FROM {{ source('dbinterface', 'gc_ATTRIBUTES') }}
WHERE 1=1