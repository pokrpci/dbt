{{ config(materialized='ephemeral') }}

SELECT [ID]
      ,[DIVISION_ID]
      ,[CREATE_AUDIT_KEY]
      ,[UPDATE_AUDIT_KEY]
      ,[DIVISION_NAME]
      ,[HOME_DIVISION]
      ,[TRACKING_HASH]
      ,[LAST_MODIFIED]
      ,[ZLOADDATE]
 FROM {{ source('dbinterface', 'gc_DIVISIONS') }}
WHERE 1=1