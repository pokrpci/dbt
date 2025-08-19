{{ config(
    materialized='incremental',
	database='DataWhs',
    alias='ConversationDetail',
    unique_key=['cvd_ConversationID','cvd_SessionID'],
	on_schema_change='ignore',
    post_hook="UPDATE tgt
      SET tgt.cvd_AgentName = src.agn_AgentName
      FROM {{ this }} AS tgt JOIN {{ ref('stg_dwhetl_agent') }} AS src
      ON tgt.cvd_AgentID    = src.agn_EmployeeID
        AND src.agn_CurrentFlag = 'Y' 
		WHERE tgt.cvd_AgentID <> ''
        AND COALESCE(tgt.cvd_AgentName, '') 
            COLLATE SQL_Latin1_General_CP1_CS_AS 
          <> COALESCE(src.agn_AgentName,'')"
) }}
		
WITH source_data AS (
-- This model is used to create the ConversationDetail table in the database.
-- It is based on the Conversations table and includes additional information from other tables.
-- The model is designed to be incremental, meaning it will only process new or updated records since the last run.
-- The model uses a combination of joins and transformations to create the final output.

	SELECT
		  CAST(t1.CONVERSATION_ID  AS VARCHAR(450)) AS cvd_ConversationID
		, CAST(t1.SESSION_ID AS VARCHAR(450)) AS cvd_SessionID
		, UPPER(LEFT(t1.SESSION_DIRECTION,1))+LOWER(SUBSTRING(t1.SESSION_DIRECTION,2,LEN(t1.SESSION_DIRECTION))) AS cvd_SessionDirection
		, t1.MEDIA_TYPE AS cvd_MediaType
		, t1.PARTICIPANT_ORDINAL AS cvd_ParticipantOrdinal
		, t1.PARTICIPANT_ID AS cvd_ParticipantID
		, t1.PURPOSE AS cvd_ParticipantPurpose
		, ISNULL(t7.EMPLOYEE_ID,'') AS cvd_AgentID
		, ISNULL(t7.RESOURCE_NAME,'') AS cvd_AgentName
		, CASE
			WHEN t1.ANI LIKE 'tel%' THEN RIGHT(LEFT(t1.ANI,PATINDEX('%[2-9]%',t1.ANI)+9),10) 
			ELSE '' END AS cvd_InboundPhoneNumber
		, CASE
			WHEN t1.SESSION_DNIS LIKE 'tel%' THEN RIGHT(LEFT(t1.SESSION_DNIS,PATINDEX('%[2-9]%',t1.SESSION_DNIS)+9),10) 
			ELSE '' END AS cvd_OutboundPhoneNumber
		, ISNULL(t1.RECORDING,'false') AS cvd_Recording
		, t3.SESSION_START AS cvd_StartDateTime
		, t3.SESSION_END AS cvd_EndDateTime
		, ISNULL(t5.RESOURCE_NAME,'') AS cvd_QueueName
		, ISNULL(t6.DIVISION_NAME,'') AS cvd_QueueDivision
		, CASE
			WHEN t1.WRAP_UP_CODE = '7fb334b0-0e9e-11e4-9191-0800200c9a66' THEN 'Default Wrap-up Code'
			ELSE COALESCE(t1.WRAP_UP_NAME,t1.WRAP_UP_CODE,'') END AS cvd_WrapUpCodeName
		, IIF(t8.RIGHT_PARTY_CONTACT IS NOT NULL,1,0) AS cvd_RPC
		, ISNULL(t1.WRAP_UP_NOTE,'') AS cvd_WrapUpNote
		, CASE
			WHEN t1.PURPOSE IN ('botflow','outbound') THEN 'none'
			ELSE ISNULL(t1.DISCONNECT_TYPE,'unknown') END AS cvd_DisconnectType

		, ISNULL(t1.CALLBACK_NUMBERS,'') AS cvd_CallbackNumbers
		, ISNULL(t1.CALLBACK_USER_NAME,'') AS cvd_CallbackName

		, ISNULL(t1.CONNECTED,0) AS cvd_CountConnected
		, ISNULL(t1.OFFERED,0) AS cvd_CountOffered
		, IIF(t1.ANSWERED_TIME IS NULL,0,1) AS cvd_CountAnswered
		, ISNULL(t1.OVER_SLA,0) AS cvd_CountOverSLA
		, IIF(t1.ABANDON_TIME IS NULL,0,1) AS cvd_CountAbandon
		, ISNULL(t1.OUTBOUND,0) AS cvd_CountOutbound
		, ISNULL(t1.ERROR,0) AS cvd_CountError
		, ISNULL(t1.OUTBOUND_ATTEMPTED,0) AS cvd_CountCampaignAttempted
		, ISNULL(t1.OUTBOUND_CONNECTED,0) AS cvd_CountCampaignConnected
		, ISNULL(t1.OUTBOUND_ABANDONED,0) AS cvd_CountCampaignAbandoned

		, ISNULL(t1.FIRST_CONNECT_TIME,0) AS cvd_SecondsFirstConnect
		, ISNULL(t1.FIRST_DIAL_TIME,0) AS cvd_SecondsFirstDial
		, ISNULL(t1.IVR_TIME,0) AS cvd_SecondsIVR
		, ISNULL(t1.ACD_TIME,0) AS cvd_SecondsACD
		, ISNULL(t1.ALERT_TIME,0) AS cvd_SecondsAlert
		, ISNULL(t1.AGENT_RESPONSE_TIME,0) AS cvd_SecondsAgentResponse
		, ISNULL(t1.USER_RESPONSE_TIME,0) AS cvd_SecondsUserResponse
		, ISNULL(t1.ANSWERED_TIME,0) AS cvd_SecondsAnswer
		, ISNULL(t1.CONTACTING_TIME,0) AS cvd_SecondsContacting
		, ISNULL(t1.DIALING_TIME,0) AS cvd_SecondsDialing
		, ISNULL(t1.TALK_TIME,0) AS cvd_SecondsTalk
		, ISNULL(t1.HELD_TIME,0) AS cvd_SecondsHeld
		, ISNULL(t9.ACW_CALC,0) AS cvd_SecondsACW  --Calc
		, (ISNULL(t1.CONTACTING_TIME,0) + ISNULL(t1.DIALING_TIME,0) + ISNULL(t1.TALK_TIME,0) + ISNULL(t1.HELD_TIME,0) + ISNULL(t9.ACW_CALC,0)) AS cvd_SecondsHandle  --Calc
		, ISNULL(t1.MONITORING_TIME,0) AS cvd_SecondsMonitoring
		, ISNULL(t1.NOT_RESPONDING_TIME,0) AS cvd_SecondsNotResponding
		, ISNULL(t1.ABANDON_TIME,0) AS cvd_SecondsAbandon
		, ISNULL(t1.SHORT_ABANDON_TIME,0) AS cvd_SecondsShortAbandon
		, ISNULL(t1.VOICE_MAIL_TIME,0) AS cvd_SecondsVoiceMail

		, ISNULL(t1.CONSULT,0) AS cvd_CountConsult
		, ISNULL(t1.TRANSFERRED,0) AS cvd_TransferredFlag
		, ISNULL(t1.TRANSFERRED_BLIND,0) AS cvd_TransferredBlindFlag
		, ISNULL(t1.TRANSFERRED_CONSULT,0) AS cvd_TransferredConsultFlag

		, ISNULL(t1.FLOW_NAME,'') AS cvd_FlowName
		, ISNULL(t1.FLOW_TYPE,'') AS cvd_FlowType
		, ISNULL(t1.FLOW_VERSION,'') AS cvd_FlowVersion
		, ISNULL(t1.FLOW_IN_TYPE,'') AS cvd_FlowInType
		, ISNULL(t1.FLOW_OUT_TYPE,'') AS cvd_FlowOutType
		, ISNULL(t1.FLOW,0) AS cvd_CountFlowIn
		, ISNULL(t1.FLOW_TIME,0) AS cvd_SecondsFlow
		, ISNULL(t1.FLOW_OUT,0) AS cvd_CountFlowOut
		, ISNULL(t1.FLOW_OUT_TIME,0) AS cvd_SecondsFlowOut
		, ISNULL(t1.FLOW_OUTCOME,0) AS cvd_CountFlowOutcome
		, ISNULL(t1.FLOW_OUTCOME_TIME,0) AS cvd_SecondsFlowOutcome
		, ISNULL(t1.ENTRY_TYPE,'') AS cvd_FlowEntryType
		, ISNULL(t1.ENTRY_REASON,'') AS cvd_FlowEntryReason
		, ISNULL(t1.EXIT_REASON,'') AS cvd_FlowExitReason
		, ISNULL(t1.TRANSFER_TYPE,'') AS cvd_FlowTransferType
		, ISNULL(t1.TRANSFER_TARGET_NAME,'') AS cvd_FlowTransferTarget  
		, t1.LAST_MODIFIED
		, t1.ZLOADDATE

   FROM  {{ ref('stg_gc_conversation_segment_fact') }} t1
   INNER JOIN {{ ref('stg_gc_date_time') }} t2 ON t1.END_DATE_TIME_KEY = t2.DATE_TIME_KEY
	LEFT JOIN {{ ref('stg_ssse') }} t3 ON t1.CONVERSATION_ID = t3.CONVERSATION_ID AND t1.SESSION_ID = t3.SESSION_ID
	LEFT JOIN {{ ref('stg_gc_queue_resources') }} t5 ON t1.QUEUE_ID = t5.RESOURCE_ID
	LEFT JOIN {{ ref('stg_gc_divisions') }} t6 ON t5.DIVISION_ID = t6.DIVISION_ID
	LEFT JOIN {{ ref('stg_gc_agent_resources') }} t7 ON t1.USER_ID = t7.RESOURCE_ID
	LEFT JOIN {{ ref('stg_gc_outbound_wrapupcode_mappings') }} t8 ON t1.WRAP_UP_CODE = t8.OUTBOUND_WRAPUPCODE_MAPPING_ID
	LEFT JOIN (
				  SELECT DISTINCT
					  CONVERSATION_ID
					, SESSION_ID
					, SUM(ACW_CALC) AS ACW_CALC
				  FROM {{ ref('stg_acw') }}
				  WHERE ACW_CALC > 0
				  GROUP BY CONVERSATION_ID, SESSION_ID) t9 ON t1.CONVERSATION_ID = t9.CONVERSATION_ID AND t1.SESSION_ID = t9.SESSION_ID
  WHERE 1=1

),
dups AS (SELECT 
			cvd_ConversationID, 
			cvd_SessionID,
			ROW_NUMBER() OVER (PARTITION BY cvd_ConversationID, cvd_SessionID ORDER BY (SELECT NULL)) AS RowNum
		FROM source_data)
		
SELECT source_data.*
  FROM source_data
 WHERE NOT EXISTS (SELECT 1
					 FROM dups
	   				WHERE dups.cvd_ConversationID = source_data.cvd_ConversationID
		 			  AND dups.cvd_SessionID = source_data.cvd_SessionID
    				  AND dups.RowNum > 1)
{% if is_incremental() %}
/*AND
    ZLOADDATE > (SELECT MAX(ZLOADDATE) FROM {{ this }})*/
{% endif %}
