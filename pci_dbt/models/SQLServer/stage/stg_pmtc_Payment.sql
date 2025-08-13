-- any light tranformations for columns
-- add original columns to the model and any tranformations needed at the end
-- if needed, add a date filter to the stage model to limit the data to be pulled from the source system
{{ config(materialized='ephemeral') }}

SELECT *
 FROM {{ source('dbinterface', 'pmtc_Payment') }}
WHERE 1=1 