-- any light tranformations for columns
-- add original columns to the model and any tranformations needed at the end
-- if needed, add a date filter to the stage model to limit the data to be pulled from the source system
{{ config(materialized='ephemeral') }}

SELECT [Id]
      ,[SentTime]
      ,[IPAddress]
      ,[CustomerId]
      ,[AccountNumber]
      ,[EmailSentTo]
      ,[EmailSubject]
      ,[EmailBody]
      ,[IsSent]
      ,[ZLOADDATE]
      ,[EmailSentTypeId]
 FROM {{ source('dbinterface', 'cspl_LogEmailSent') }}
WHERE 1=1 