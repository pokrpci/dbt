-- any light tranformations for columns
-- add original columns to the model and any tranformations needed at the end
-- if needed, add a date filter to the stage model to limit the data to be pulled from the source system
{{ config(materialized='ephemeral') }}

SELECT
    sd.StartDate,
    sd.EndDate,
    sd.EndType,
    sd.TotalAmount AS PaymentAmount,
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
    sd.ZBUILDDATE,
    sd.ZLOADDATE
FROM (
    SELECT 
         pr.RecurringID,
         pr.FirstPaymentDate     AS StartDate,
        NULLIF(
          CASE 
            WHEN pr.RunUntilDate IS NULL THEN NULL
            WHEN CAST(pr.RunUntilDate AS DATE) = '1900-01-01' THEN NULL
            ELSE pr.RunUntilDate 
          END
        , '')                   AS EndDate,
        CASE 
          WHEN pr.RunUntilDate IS NULL 
               OR CAST(pr.RunUntilDate AS DATE) = '1900-01-01'
            THEN 'PAID IN FULL'
          ELSE 'DATE'
        END                      AS EndType,
        pr.TotalAmount,
        pr.Frequency,
        COALESCE(
          NULLIF(pr.RoutingNumber, ''),
          '0' + pr.TransitNumber + pr.FinancialInstitutionNumber
        )                         AS RoutingNumber,
        pr.BankName,
        pr.CheckingSavings,
        ''                        AS CardType,
        ''                        AS CardBrand,
        'BANK'                    AS PaymentMethod,
        pr.AccountNumber,
        pr.cpoststat              AS PaymentScheduleStatusCode,
        CASE pr.cpoststat
          WHEN '0000' THEN 'Active'
          WHEN '1000' THEN 'Recurring complete (frequency Once only)'
          WHEN '1001' THEN 'Recurring complete (Run Until Date)'
          WHEN '1002' THEN 'Recurring complete (Balance <=0)'
          WHEN '5000' THEN 'Invalid Frequency'
          WHEN '5001' THEN 'Canceled'
          ELSE 'Unknown'
        END                       AS PaymentScheduleStatus,
        pr.EnteredBy,
        pr.RecurringID            AS SrcId,
        pr.ServSystem,
        pr.BankName               AS ZBankName,
        pr.ZLOADDATE              AS ZBUILDDATE,
        GETDATE()                 AS ZLOADDATE
    FROM 
        {{ ref('stg_ppay_Recurring') }} pr
) sd
LEFT JOIN
    {{ ref('stg_BankName') }} bn
      ON sd.RoutingNumber = bn.RoutingNumber
WHERE
    sd.RecurringID > 0
    AND sd.ServSystem <> 'SETT'