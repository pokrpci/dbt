{{ config(materialized='ephemeral') }}
SELECT
		q1.CONVERSATION_ID,
		q4.RESOURCE_NAME AS QUEUE_NAME,
		MIN(q1.PARTICIPANT_ORDINAL) AS QUEUE_ORDER
	FROM  {{ ref('stg_gc_conversation_segment_fact') }} q1
	INNER JOIN {{ ref('stg_gc_queue_resources') }} q4 ON q1.QUEUE_ID = q4.RESOURCE_ID
	WHERE q1.QUEUE_ID IS NOT NULL
	GROUP BY q1.CONVERSATION_ID, q4.RESOURCE_NAME