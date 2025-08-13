{{ config(materialized='ephemeral') }}

SELECT [ID]
      ,[FORM_ID]
      ,[CREATE_AUDIT_KEY]
      ,[UPDATE_AUDIT_KEY]
      ,[NAME]
      ,[MODIFIED_DATE]
      ,[PUBLISHED]
      ,[CONTEXT_ID]
      ,[WEIGHT_MODE]
      ,[TRACKING_HASH]
      ,[LAST_MODIFIED]
      ,[ZLOADDATE]
 FROM {{ source('dbinterface', 'gc_FORMS') }}
WHERE 1=1