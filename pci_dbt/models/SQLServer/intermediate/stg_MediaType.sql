{{ config(materialized='ephemeral') }}
SELECT
		m1.CONVERSATION_ID,
		m1.MEDIA_TYPE,
		MIN(m1.PARTICIPANT_ORDINAL) AS TYPE_ORDER
	FROM  {{ ref('stg_gc_conversation_segment_fact') }}  m1
	GROUP BY m1.CONVERSATION_ID, m1.MEDIA_TYPE