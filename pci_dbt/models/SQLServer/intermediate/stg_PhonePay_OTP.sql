-- any light tranformations for columns
-- add original columns to the model and any tranformations needed at the end
-- if needed, add a date filter to the stage model to limit the data to be pulled from the source system
{{ config(materialized='ephemeral') }}

SELECT    
  sd.StartDate,
  sd.EndDate,
  sd.EndType,
  sd.PaymentAmount,
  sd.Frequency,
  sd.RoutingNumber,
  bn.BankName,
  sd.CheckingSavings,
  sd.CardType,   
  sd.CardBrand,
  sd.PaymentMethod,
  sd.AccountNumber,
  sd.PaymentScheduleStatus,
  sd.EnteredBy,
  sd.SrcId,
  sd.ServSystem,
  sd.ZBankName,
  sd.ZBuildDate,
  sd.ZLoadDate
FROM (
  SELECT 
     pp.RecurringID,
     pp.PayAppPaymentMethodId,
     pp.PaymentDate           AS StartDate,
     pp.PaymentDate           AS EndDate,
     'DATE'                   AS EndType,
     pp.PaymentAmount,
     'ONCE'                   AS Frequency,
     COALESCE(
       NULLIF(pp.RoutingNumber, ''),
       '0' + pp.TransitNumber + pp.FinancialInstitutionNumber
     )                         AS RoutingNumber,   
     pp.CheckingSavings,
     ''                        AS CardType,
     ''                        AS CardBrand,
     'BANK'                    AS PaymentMethod,
     pp.AccountNumber,
     'Complete'               AS PaymentScheduleStatus,
     pp.EnteredBy,
     pp.Id                     AS SrcId,
     pp.ServSystem,
     pp.BankName               AS ZBankName,
     pp.ZLOADDATE              AS ZBuildDate,
     GETDATE()                 AS ZLoadDate
  FROM 
    {{ ref('stg_ppay_PhonePayments') }} pp
) sd
LEFT JOIN
  {{ ref('stg_ppay_Recurring') }} pr
    ON sd.RecurringID = pr.RecurringID
LEFT JOIN
  {{ ref('stg_BankName') }} bn
    ON sd.RoutingNumber = bn.RoutingNumber

WHERE
  (sd.RecurringID IS NULL OR sd.RecurringID = 0)
  AND sd.PaymentAmount <> 0      -- skip prenotes
  AND sd.PayAppPaymentMethodId IS NULL
  AND sd.ServSystem <> 'SETT'
  AND CAST(sd.EndDate AS DATE) <= '2022-06-28'