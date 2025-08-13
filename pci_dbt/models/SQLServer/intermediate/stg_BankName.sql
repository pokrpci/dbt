-- any light tranformations for columns
-- add original columns to the model and any tranformations needed at the end
-- if needed, add a date filter to the stage model to limit the data to be pulled from the source system
{{ config(materialized='ephemeral') }}

SELECT
  t.RoutingNumber,
  t.BankName,
  t.UsageCount
FROM (
  SELECT
    pp.RoutingNumber,
    pp.BankName,
    COUNT(*) AS UsageCount,
    RANK() OVER (
      PARTITION BY pp.RoutingNumber
      ORDER BY COUNT(*) DESC
    ) AS UsageRank
  FROM DBINTERFACE.dbo.ppay_PhonePayments AS pp
  WHERE pp.RoutingNumber IS NOT NULL
  GROUP BY
    pp.RoutingNumber,
    pp.BankName
) AS t
WHERE
  t.UsageRank = 1

/*WITH BankCounts AS (
  SELECT 
    RoutingNumber,
    BankName,
    COUNT(*) AS UsageCount
  FROM {{ ref('stg_ppay_PhonePayments') }} 
  WHERE RoutingNumber IS NOT NULL
  GROUP BY RoutingNumber, BankName
 -- order by BankName
), 
TopBankName AS (
SELECT
  RoutingNumber,
  BankName,
  UsageCount,
  RANK() OVER (
    PARTITION BY RoutingNumber 
    ORDER BY UsageCount DESC
  ) AS UsageRank
FROM BankCounts
--ORDER BY RoutingNumber, UsageRank
)
SELECT * FROM TopBankName
WHERE UsageRank = 1
*/