-- This model processes recurring payment schedules from the Payment Central system.
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
    CAST(ps.PaymentStartDate AS DATE)                                    AS StartDate,
    CAST(ps.PaymentEndDate   AS DATE)                                    AS EndDate,
    CASE 
      WHEN ps.PaymentEndDate IS NULL THEN 'PAID IN FULL' 
      ELSE 'DATE' 
    END                                                                  AS EndType,
    ps.PaymentAmount                                                     AS PaymentAmount,
    rt.DisplayName                                                       AS Frequency,
    COALESCE(
      ubt.RoutingNumber,
      '0' + cbt.FinancialInstitutionNumber + cbt.TransitNumber
    )                                                                    AS RoutingNumber,
    COALESCE(cbt.BankName, ubt.BankName)                                 AS BankName,
    ISNULL(ubt.BankAccountType, '')                                      AS CheckingSavings,
    ct1.DisplayName                                                      AS CardType,
    cb.DisplayName                                                       AS CardBrand,
    pmt.DisplayName                                                      AS PaymentMethod,
    COALESCE(
      ps.AccountNumber,
      ubt.AccountNumber,
      cbt.AccountNumber
    )                                                                    AS AccountNumber,
    pss.DisplayName                                                      AS PaymentScheduleStatus,
    ps.CreatedBy                                                         AS EnteredBy,
    ps.Id                                                                AS SrcId,
    ps.ServSystem                                                        AS ServSystem,
    COALESCE(cbt.BankName, ubt.BankName)                                 AS ZBankName,
    ps.ZLOADDATE                                                         AS ZBuildDate,
    GETDATE()                                                            AS ZLoadDate
  FROM {{ ref('stg_pmtc_PaymentSchedule') }}              ps
  INNER JOIN {{ ref('stg_pmtc_PaymentMethod') }}          pm    ON ps.PaymentMethodId         = pm.Id
  LEFT  JOIN {{ ref('stg_pmtc_CardToken') }}              ct    ON ct.Id                      = pm.CardTokenId
  LEFT  JOIN {{ ref('stg_pmtc_CardType') }}               ct1   ON ct1.Id                     = ct.CardTypeId
  LEFT  JOIN {{ ref('stg_pmtc_CardBrand') }}              cb    ON cb.Id                      = ct.CardBrandId  
  LEFT  JOIN {{ ref('stg_pmtc_CanadianBankToken') }}      cbt   ON cbt.Id                     = pm.CanadianBankTokenId
  LEFT  JOIN {{ ref('stg_pmtc_UsaBankToken') }}           ubt   ON ubt.Id                     = pm.UsaBankTokenId
  INNER JOIN {{ ref('stg_pmtc_PaymentMethodType') }}      pmt   ON pm.PaymentMethodTypeId     = pmt.Id
  LEFT  JOIN {{ ref('stg_pmtc_RecurrenceType') }}         rt    ON ps.RecurrenceTypeId        = rt.Id
  LEFT  JOIN {{ ref('stg_pmtc_PaymentScheduleStatus') }}  pss   ON ps.PaymentScheduleStatusId = pss.Id
  WHERE
    ISNULL(ps.ImportSourceTypeId, 0) NOT IN (2, 3)
    AND ps.ServSystem            <> 'SETT'
) sd
LEFT JOIN
  {{ ref('stg_BankName') }} bn
    ON sd.RoutingNumber = bn.RoutingNumber
