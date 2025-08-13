	{{ config(materialized='ephemeral') }}
	
	SELECT
		d1.CONVERSATION_ID,
		d5.DIVISION_NAME,
		MIN(d1.PARTICIPANT_ORDINAL) AS DIVISION_ORDER
	FROM {{ ref('stg_gc_conversation_segment_fact') }} d1
	INNER JOIN {{ ref('stg_gc_queue_resources') }} d4 ON d1.QUEUE_ID = d4.RESOURCE_ID
	INNER JOIN {{ ref('stg_gc_divisions') }} d5 ON d4.DIVISION_ID = d5.DIVISION_ID
	WHERE d1.QUEUE_ID IS NOT NULL
	GROUP BY d1.CONVERSATION_ID, d5.DIVISION_NAME
	