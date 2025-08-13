{{ config(materialized='ephemeral') }}

SELECT  [ID]
      ,[Code]
      ,[ConversationDate]
      ,[OverrideDate]
      ,[AccountNumber]
      ,[EnterDateTime]
      ,[EnterUserName]
      ,[LastChgDateTime]
      ,[LastChgUserName]
      ,[ZLOADDATE] 
 FROM {{ source('dbinterface', 'mds_GenesysAccountNumberOverride') }}
WHERE 1=1