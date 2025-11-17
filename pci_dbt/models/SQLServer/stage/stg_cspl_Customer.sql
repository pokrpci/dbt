-- any light tranformations for columns
-- add original columns to the model and any tranformations needed at the end
-- if needed, add a date filter to the stage model to limit the data to be pulled from the source system
{{ config(materialized='ephemeral') }}

SELECT [Id]
      ,[UserName]
      ,[AccessFailedCount]
      ,[LockoutEnabled]
      ,[LockoutEnd]
      ,[PasswordHash]
      ,[TwoFactorEnabled]
      ,[CreatedTime]
      ,[PasswordLastUpdateTime]
      ,[LastSigninTime]
      ,[ZLOADDATE]
 FROM {{ source('dbinterface', 'cspl_Customer') }}
WHERE 1=1 