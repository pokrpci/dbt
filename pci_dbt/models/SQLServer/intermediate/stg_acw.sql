{{ config(materialized='ephemeral') }}

-- This model calculates the After Call Work (ACW) time for each conversation.
	SELECT
		t1.CONVERSATION_ID,
		t1.SESSION_ID,
		CONVERT(INT, ROUND(DATEDIFF(ms, w1.WRAP_UP_START, w2.WRAP_UP_END) * 1.0 / 1000, 0)) AS ACW_CALC
	FROM {{ ref('stg_gc_conversation_segment_fact') }} t1
	LEFT JOIN (
		SELECT  
			t1.CONVERSATION_ID,
			t1.SESSION_ID,
			MAX(t1.SEGMENT_END) AS WRAP_UP_START
		FROM  {{ ref('stg_gc_session_segments_fact') }} t1
		INNER JOIN {{ ref('stg_gc_conversation_segment_fact') }} t2 ON t1.SESSION_ID = t2.SESSION_ID
		AND t2.PURPOSE = 'agent'
		AND t1.SEGMENT_TYPE <> 'wrapup'
		GROUP BY t1.CONVERSATION_ID, t1.SESSION_ID
	) w1 ON t1.CONVERSATION_ID = w1.CONVERSATION_ID AND t1.SESSION_ID = w1.SESSION_ID
	LEFT JOIN (
		SELECT  
			t1.CONVERSATION_ID,
			t1.SESSION_ID,
			MAX(t1.SEGMENT_END) AS WRAP_UP_END
		FROM  {{ ref('stg_gc_session_segments_fact') }} t1
		INNER JOIN {{ ref('stg_gc_conversation_segment_fact') }} t2 ON t1.SESSION_ID = t2.SESSION_ID
		AND t2.PURPOSE = 'agent'
		AND t1.SEGMENT_TYPE = 'wrapup'
		GROUP BY t1.CONVERSATION_ID, t1.SESSION_ID
	) w2 ON t1.CONVERSATION_ID = w2.CONVERSATION_ID AND t1.SESSION_ID = w2.SESSION_ID
	WHERE 1=1 
	AND t1.PURPOSE = 'agent'

