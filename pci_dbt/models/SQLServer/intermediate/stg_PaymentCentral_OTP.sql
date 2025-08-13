{{ config(materialized='ephemeral') }}

SELECT 
  OneOffPayments.StartDate,
  OneOffPayments.EndDate,
  OneOffPayments.EndType,
  OneOffPayments.PaymentAmount,
  OneOffPayments.Frequency,
  OneOffPayments.RoutingNumber,
  bn.BankName,
  OneOffPayments.CheckingSavings,
  OneOffPayments.CardType,
  OneOffPayments.CardBrand,
  OneOffPayments.PaymentMethod,
  OneOffPayments.AccountNumber,
  CAST(OneOffPayments.PaymentScheduleStatus AS VARCHAR(100)) AS PaymentScheduleStatus,
  OneOffPayments.EnteredBy,
  OneOffPayments.SrcId,
  OneOffPayments.ServSystem,
  OneOffPayments.zBankName,
  OneOffPayments.ZBuildDate,
  OneOffPayments.ZLoadDate
FROM (
  SELECT
    p.PaymentDate                                                     AS StartDate,
    p.PaymentDate                                                     AS EndDate,
    'DATE'                                                            AS EndType,
    p.PaymentAmount                                                   AS PaymentAmount,
    'ONCE'                                                            AS Frequency,
    COALESCE(
      ubt.RoutingNumber,
      '0' + cbt.FinancialInstitutionNumber + cbt.TransitNumber
    )                                                                 AS RoutingNumber,
    COALESCE(cbt.BankName, ubt.BankName)                              AS zBankName,
    ISNULL(ubt.BankAccountType, '')                                   AS CheckingSavings,
    ct1.DisplayName                                                   AS CardType,
    cb.DisplayName                                                    AS CardBrand,
    pmt.DisplayName                                                   AS PaymentMethod,
    COALESCE(
      pm.AccountNumber,
      ubt.AccountNumber,
      cbt.AccountNumber
    )                                                                 AS AccountNumber,
    NULL                                                              AS PaymentScheduleStatus,
    pm.CreatedBy                                                      AS EnteredBy,
    p.Id                                                              AS SrcId,
    p.ServSystem                                                      AS ServSystem,
    p.ZLOADDATE                                                       AS ZBuildDate,
    GETDATE()                                                         AS ZLoadDate
  FROM
    {{ ref('stg_pmtc_Payment') }}                                     AS p
    INNER JOIN {{ ref('stg_pmtc_PaymentMethod') }}                    AS pm  ON p.PaymentMethodId       = pm.Id
    LEFT  JOIN {{ ref('stg_pmtc_CardToken') }}                        AS ct  ON ct.Id             = pm.CardTokenId
    LEFT  JOIN {{ ref('stg_pmtc_CardType') }}                         AS ct1 ON ct1.Id            = ct.CardTypeId
    LEFT  JOIN {{ ref('stg_pmtc_CardBrand') }}                        AS cb  ON cb.Id             = ct.CardBrandId
    LEFT  JOIN {{ ref('stg_pmtc_CanadianBankToken') }}                AS cbt ON cbt.Id            = pm.CanadianBankTokenId
    LEFT  JOIN {{ ref('stg_pmtc_UsaBankToken') }}                     AS ubt ON ubt.Id            = pm.UsaBankTokenId
    INNER JOIN {{ ref('stg_pmtc_PaymentMethodType') }}                AS pmt ON pm.PaymentMethodTypeId = pmt.Id
  WHERE
    p.PaymentScheduleId IS NULL
    AND p.ServSystem       <> 'SETT'
) AS OneOffPayments
LEFT JOIN
  {{ ref('stg_BankName') }} bn
    ON OneOffPayments.RoutingNumber = bn.RoutingNumber