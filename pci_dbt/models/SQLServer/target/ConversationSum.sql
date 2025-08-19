{% set utc_now = modules.datetime.datetime.utcnow() %}
{% set ct_now  = utc_now - modules.datetime.timedelta(hours=5) %}
{% set ct_hr   = ct_now.hour %}
{% set run_hook = ct_hr == 21 %}  -- 9 PM CT

{{ config(
    materialized='incremental',incremental_strategy='merge',
	database='DataWhs',
    alias='ConversationSummary',
    unique_key=['cvs_ConversationID'],
	on_schema_change='ignore',
    post_hook= run_hook 
      and ["
      UPDATE tgt
      SET tgt.cvs_Agents = src.cvs_Agents
      FROM {{ this }} AS tgt
      JOIN {{ ref('stg_Agents_all') }} AS src
        ON tgt.cvs_ConversationID = src.cvs_ConversationID
      WHERE tgt.cvs_Agents COLLATE SQL_Latin1_General_CP1_CS_AS <> src.cvs_Agents " ]
      or []
) }}
				
WITH source_data AS (
SELECT 
		f1.CONVERSATION_ID AS cvs_ConversationID,  -- Conversation ID
		f1.CONVERSATION_START AS cvs_StartDateTime,  -- Start date and time of the conversation
		f1.CONVERSATION_END AS cvs_EndDateTime,  -- End date and time of the conversation
		--f1.START_INTERVAL AS cvs_StartInterval,  -- Start interval
		CONCAT(REPLACE(t.LABEL_HH24,'24','00'),':',t.LABEL_30MI) cvs_StartInterval,  -- Start interval  
		f1.DURATION_SECONDS AS cvs_TotalSeconds,  -- Total duration in seconds
		f1.DIRECTION AS cvs_OriginatingDirection,  -- Direction of the conversation
		CAST(COALESCE(f4.AcctNbr, f5.AcctNbr, '') AS VARCHAR(100)) AS cvs_AcctNbr,  -- Account number
		ISNULL(f6.DistNbr, '') AS cvs_DistNbr,  -- Distribution number
		TRIM(ISNULL(STUFF((SELECT ', ' + div.DIVISION_NAME 
					  FROM {{ ref('stg_Divisions') }}  div
					  WHERE f1.CONVERSATION_ID = div.CONVERSATION_ID
					  ORDER BY div.DIVISION_ORDER
					  FOR XML PATH('')), 1, 1, ''), f2.DIVISION_NAME)) AS cvs_Divisions,  -- Divisions involved
		TRIM(ISNULL(STUFF((SELECT ', ' + que.QUEUE_NAME 
					  FROM {{ ref('stg_Queues') }}  que
					  WHERE f1.CONVERSATION_ID = que.CONVERSATION_ID
					  ORDER BY que.QUEUE_ORDER
					  FOR XML PATH('')), 1, 1, ''), '')) AS cvs_Queues,  -- Queues involved
		TRIM(STUFF((SELECT ', ' + mt.MEDIA_TYPE 
			   FROM {{ ref('stg_MediaType') }}  mt
			   WHERE f1.CONVERSATION_ID = mt.CONVERSATION_ID
			   ORDER BY mt.TYPE_ORDER
			   FOR XML PATH('')), 1, 1, '')) AS cvs_MediaTypes,  -- Media types involved
		TRIM(STUFF((SELECT ', ' + p.PURPOSE 
			   FROM {{ ref('stg_Participants') }}  p
			   WHERE f1.CONVERSATION_ID = p.CONVERSATION_ID
			   ORDER BY p.PURPOSE_ORDER
			   FOR XML PATH('')), 1, 1, '')) AS cvs_Participants,  -- Participants involved
		TRIM(ISNULL(STUFF((SELECT ', ' + agt.AGENT_NAME 
					  FROM {{ ref('stg_Agents') }}  agt
					  WHERE f1.CONVERSATION_ID = agt.CONVERSATION_ID
					  ORDER BY agt.AGENT_ORDER
					  FOR XML PATH('')), 1, 1, ''), '')) AS cvs_Agents,  -- Agents involved
		TRIM(ISNULL(f3.WRAP_UP, '')) AS cvs_FinalWrapupCode,  -- Final wrap-up code
		ISNULL(f3.RPC, 0) AS cvs_RPC,  -- Right party contact
		f1.CALL_METHOD AS cvs_CallMethod,  -- Call method (Dialer or Manual)
		--f1.LOCALIZED_FLAG cvs_LocalizedFlag,
		f1.CAMPAIGN_NAME AS cvs_CampaignName,  -- Campaign name
		f1.DIALING_MODE AS cvs_DialingMode,  -- Dialing mode
		f1.CONTACT_LIST_ID AS cvs_ContactListID,  -- Contact list ID
		ISNULL(IIF(f1.CALL_METHOD = 'Dialer', 0, f7.CALLBACK), 0) AS cvs_CallbackRequested,  -- Callback requested
		f1.FLOW AS cvs_CountFlow,  -- Flow count
		f1.OUTBOUND AS cvs_CountOutbound,  -- Outbound count
		f1.CAMPAIGN_ATTEMPTED AS cvs_CountCampaignAttempted,  -- Campaign attempted count
		f1.CAMPAIGN_CONNECTED AS cvs_CountCampaignConnected,  -- Campaign connected count
		f1.CAMPAIGN_ABANDONED AS cvs_CountCampaignAbandoned,  -- Campaign abandoned count
		f1.OFFERED AS cvs_CountOffered,  -- Offered count
		f1.ANSWERED AS cvs_CountAnswered,  -- Answered count
		f1.CONNECTED AS cvs_CountConnected,  -- Connected count
		f1.ERROR AS cvs_CountError,  -- Error count
		f1.OVER_SLA AS cvs_CountOverSLA,  -- Over SLA count
		f1.CONSULT AS cvs_CountConsult,  -- Consult count
		f1.TRANSFERRED AS cvs_CountTransferred,  -- Transferred count
		f1.TRANSFERRED_CONSULT AS cvs_CountTransferredConsult,  -- Transferred consult count
		f1.TRANSFERRED_BLIND AS cvs_CountTransferredBlind,  -- Transferred blind count
		f1.IVR_TIME AS cvs_SecondsIVR,  -- IVR time in seconds
		f1.ABANDON_TIME AS cvs_SecondsAbandon,  -- Abandon time in seconds
		f1.ACD_TIME AS cvs_SecondsACD,  -- ACD time in seconds
		f1.ALERT_TIME AS cvs_SecondsAlert,  -- Alert time in seconds
		f1.AGENT_RESPONSE_TIME AS cvs_SecondsAgentResponse,  -- Agent response time in seconds
		f1.CONTACTING_TIME AS cvs_SecondsContacting,  -- Contacting time in seconds
		f1.DIALING_TIME AS cvs_SecondsDialing,  -- Dialing time in seconds
		f1.FLOW_TIME AS cvs_SecondsFlow,  -- Flow time in seconds
		f1.ANSWERED_TIME AS cvs_SecondsAnswered,  -- Answered time in seconds
		f1.TALK_TIME AS cvs_SecondsTalk,  -- Talk time in seconds
		f1.HELD_TIME AS cvs_SecondsHeld,  -- Held time in seconds
		ISNULL(f8.ACW_CALC, 0) AS cvs_SecondsACW,  -- ACW time in seconds (calculated)
		(f1.CONTACTING_TIME + f1.DIALING_TIME + f1.TALK_TIME + f1.HELD_TIME + ISNULL(f8.ACW_CALC, 0)) AS cvs_SecondsHandle,  -- Handle time in seconds
		f1.LAST_MODIFIED,
		f1.ZLOADDATE,
		f1.LOCALIZED_FLAG cvs_LocalizedFlag
	FROM {{ ref('stg_ConversationCon') }} f1
	INNER JOIN {{ ref('stg_gc_date_time') }} t ON f1.M_START_DATE_TIME_KEY = t.DATE_TIME_KEY
	LEFT JOIN {{ ref('stg_gc_divisions') }} f2 ON f1.DIVISION_ID_2 = f2.DIVISION_ID
	LEFT JOIN {{ ref('stg_WrapUp') }}  f3 ON f1.CONVERSATION_ID = f3.CONVERSATION_ID AND f3.RN_DESC = 1
	LEFT JOIN (
			SELECT
			   Code
			  ,OverrideDate
			  ,AccountNumber AS AcctNbr
			  ,ROW_NUMBER() OVER (PARTITION BY Code ORDER BY OverrideDate DESC) AS RN
			 FROM {{ ref('stg_mds_GenesysAccountNumberOverride') }}  
	) f4 ON f1.CONVERSATION_ID = f4.Code AND f4.RN = 1
	LEFT JOIN (
		SELECT
			CONVERSATION_ID,
			PARTICIPANT_PURPOSE,
			VALUE AS AcctNbr,
			ROW_NUMBER() OVER (PARTITION BY CONVERSATION_ID ORDER BY 
				CASE PARTICIPANT_PURPOSE 
					WHEN 'agent' THEN 1 
					WHEN 'customer' THEN 2 
					WHEN 'external' THEN 3 
					WHEN 'acd' THEN 4 
					ELSE 5 
				END) AS RN
		FROM {{ ref('stg_gc_attributes') }} t1
		WHERE KEYNAME = 'Account Number'
		AND LEN(VALUE) BETWEEN 2 AND 18
	) f5 ON f1.CONVERSATION_ID = f5.CONVERSATION_ID AND f5.RN = 1
	LEFT JOIN (
		SELECT
			CONVERSATION_ID,
			PARTICIPANT_PURPOSE,
			VALUE AS DistNbr,
			ROW_NUMBER() OVER (PARTITION BY CONVERSATION_ID ORDER BY 
				CASE PARTICIPANT_PURPOSE 
					WHEN 'agent' THEN 1 
					WHEN 'customer' THEN 2 
					WHEN 'acd' THEN 3 
					WHEN 'external' THEN 4 
					ELSE 5 
				END) AS RN
		FROM {{ ref('stg_gc_attributes') }} t1
		WHERE KEYNAME = 'Client Number'
		AND LEN(VALUE) BETWEEN 2 AND 18
	) f6 ON f1.CONVERSATION_ID = f6.CONVERSATION_ID AND f6.RN = 1
	LEFT JOIN (
		SELECT DISTINCT
			CONVERSATION_ID,
			1 AS CALLBACK
		FROM {{ ref('stg_gc_conversation_segment_fact') }} 
		WHERE CALLBACK_NUMBERS IS NOT NULL
	) f7 ON f1.CONVERSATION_ID = f7.CONVERSATION_ID
	LEFT JOIN (
		SELECT DISTINCT
			CONVERSATION_ID,
			SUM(ACW_CALC) AS ACW_CALC
		FROM {{ ref('stg_acw') }} 
		WHERE ACW_CALC > 0
		GROUP BY CONVERSATION_ID
	) f8 ON f1.CONVERSATION_ID = f8.CONVERSATION_ID
),
dups AS (SELECT 
			cvs_ConversationID, 
			ROW_NUMBER() OVER (PARTITION BY cvs_ConversationID ORDER BY (SELECT NULL)) AS RowNum
		FROM source_data)
		
SELECT source_data.*
  FROM source_data
 WHERE EXISTS (SELECT 1
					 FROM dups
	   				WHERE dups.cvs_ConversationID = source_data.cvs_ConversationID
    				  AND dups.RowNum = 1)

{% if is_incremental() %}
 /*AND
    ZLOADDATE > (SELECT MAX(ZLOADDATE) FROM {{ this }})*/
{% endif %}
