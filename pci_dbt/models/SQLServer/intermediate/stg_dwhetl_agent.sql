{{ config(materialized='view') }}

SELECT agn_AgentID
      ,agn_Version
      ,agn_StartTime
      ,agn_AgentName
      ,agn_EmployeeID
      ,agn_Dept
      ,agn_SubDept
      ,agn_Status
      ,agn_EffectiveTime
      ,agn_CurrentFlag
      ,agn_NotExists
      ,ZSrce
      ,ZSrceDate
FROM "DataWhsETL"."dbo"."Agent"
    


