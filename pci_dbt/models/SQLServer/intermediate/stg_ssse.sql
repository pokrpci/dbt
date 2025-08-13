{{ config(materialized='ephemeral') }}
	SELECT 
		s1.CONVERSATION_ID,
		s1.SESSION_ID,
		MIN(DATEADD(hh, CONVERT(INT, s2.LABEL_TZ), s1.SEGMENT_START)) AS SESSION_START,
		MAX(DATEADD(hh, CONVERT(INT, s3.LABEL_TZ), s1.SEGMENT_END)) AS SESSION_END  
	FROM {{ ref('stg_gc_conversation_segment_fact') }} s1
	INNER JOIN {{ ref('stg_gc_date_time') }} s2 
		ON s1.START_DATE_TIME_KEY = s2.DATE_TIME_KEY
	INNER JOIN {{ ref('stg_gc_date_time') }} s3 
		ON s1.END_DATE_TIME_KEY = s3.DATE_TIME_KEY
    WHERE 1=1	     
	    AND s1.CONVERSATION_ID in (select distinct CONVERSATION_ID FROM {{ ref('stg_gc_conversation_segment_fact') }})
	GROUP BY 
		s1.CONVERSATION_ID, 
		s1.SESSION_ID