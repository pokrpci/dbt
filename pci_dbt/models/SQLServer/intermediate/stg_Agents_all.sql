{{ config(materialized='table') }}

SELECT
  cso.cvs_ConversationID,
  ISNULL(
    STUFF((
      SELECT
        ', ' + ar.resource_name
      FROM "DataWhs"."dbo"."ConversationDetail" AS cd
      INNER JOIN {{ ref('stg_gc_agent_resources') }} AS ar
        ON cd.cvd_AgentID = ar.EMPLOYEE_ID
      WHERE cd.cvd_ConversationID = cso.cvs_ConversationID
      GROUP BY cd.cvd_ConversationID, ar.resource_name
      ORDER BY MIN(cd.cvd_ParticipantOrdinal)
      FOR XML PATH(''), TYPE
    ).value('.', 'nvarchar(max)'), 1, 2, ''),
    ''
  ) AS cvs_Agents
FROM "DataWhs"."dbo"."ConversationSummary" AS cso