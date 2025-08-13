{{ config(materialized='ephemeral') }}
SELECT
		p1.CONVERSATION_ID,
		p1.PURPOSE,
		MIN(p1.PARTICIPANT_ORDINAL) AS PURPOSE_ORDER
	FROM  {{ ref('stg_gc_conversation_segment_fact') }}  p1
	GROUP BY p1.CONVERSATION_ID, p1.PURPOSE