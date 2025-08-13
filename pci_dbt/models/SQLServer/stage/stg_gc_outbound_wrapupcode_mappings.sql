{{ config(materialized='ephemeral') }}

SELECT [ID]
      ,[OUTBOUND_WRAPUPCODE_MAPPING_ID]
      ,[CREATE_AUDIT_KEY]
      ,[UPDATE_AUDIT_KEY]
      ,[NAME]
      ,[CONTACT_UNCALLABLE]
      ,[NUMBER_UNCALLABLE]
      ,[RIGHT_PARTY_CONTACT]
      ,[DATE_MODIFIED]
      ,[TRACKING_HASH]
      ,[LAST_MODIFIED]
      ,[ZLOADDATE] 
 FROM {{ source('dbinterface', 'gc_OUTBOUND_WRAPUPCODE_MAPPINGS') }}
WHERE 1=1