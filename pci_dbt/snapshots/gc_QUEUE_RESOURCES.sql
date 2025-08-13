{% snapshot queue_snapshot %}

{{
    config(
        target_schema='dbo',
        unique_key='ID',
        strategy='timestamp',
        updated_at='ZLOADDATE',
        pre_hook="SET IDENTITY_INSERT {{ this }} ON",
        post_hook="SET IDENTITY_INSERT {{ this }} OFF"
    )
}}

 SELECT [ID]
      ,[RESOURCE_ID]
      ,[RESOURCE_NAME]
      ,[RESOURCE_TYPE]
      ,[ACTIVE_FLAG]
      ,[EFFECTIVE_DATE]
      ,[EXPIRATION_DATE]

      ,[CHAT_SL_DURATION]
	  ,[CHAT_SL_PERCENTAGE]
	  ,[CALL_SL_PERCENTAGE]
	  ,[CALL_SL_DURATION]
      ,[EMAIL_SL_DURATION]
	  ,[EMAIL_SL_PERCENTAGE]

      ,[ZLOADDATE]
  FROM  [DBINTERFACE].[dbo].[gc_QUEUE_RESOURCES]


{% endsnapshot %}