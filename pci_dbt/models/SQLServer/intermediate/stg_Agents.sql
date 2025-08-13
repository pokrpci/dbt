{{ config(materialized='ephemeral') }}

SELECT
		a1.CONVERSATION_ID,
		a4.RESOURCE_NAME AGENT_NAME,
		MIN(a1.PARTICIPANT_ORDINAL) AS AGENT_ORDER
	FROM {{ ref('stg_gc_conversation_segment_fact') }} a1
	INNER JOIN {{ ref('stg_gc_agent_resources') }} a4 ON a1.USER_ID = a4.RESOURCE_ID
	GROUP BY a1.CONVERSATION_ID, a4.RESOURCE_NAME