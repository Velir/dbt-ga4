with last_non_direct_session_partition_key as (
  select
    client_key
    ,session_partition_key
    ,session_partition_date
    ,session_source
    ,session_medium
    ,session_source_category
    ,session_campaign
    ,session_content
    ,session_term
    ,session_default_channel_grouping
    ,non_direct_session_partition_key
    ,CASE
      WHEN non_direct_session_partition_key is null
      THEN 
          last_value(non_direct_session_partition_key ignore nulls) over(
          partition by client_key
          order by
              UNIX_MICROS(TIMESTAMP(session_partition_date)) range between 2592000000000 preceding --TODO need to work with session start timestamp, not date
              and current row -- 30 day lookback
          )
      ELSE non_direct_session_partition_key
    END as session_partition_key_last_non_direct,
  from
    {{ref('stg_ga4__sessions_traffic_sources_daily')}}
)
,join_last_non_direct_session_source as (
  select
    last_non_direct_session_partition_key.client_key
    ,last_non_direct_session_partition_key.session_partition_key
    ,last_non_direct_session_partition_key.session_partition_date
    ,last_non_direct_session_partition_key.session_source
    ,last_non_direct_session_partition_key.session_medium
    ,last_non_direct_session_partition_key.session_source_category
    ,last_non_direct_session_partition_key.session_campaign
    ,last_non_direct_session_partition_key.session_content
    ,last_non_direct_session_partition_key.session_term
    ,last_non_direct_session_partition_key.session_default_channel_grouping
    ,last_non_direct_session_partition_key.session_partition_key_last_non_direct
    ,last_non_direct_source.session_source as session_source_last_non_direct
    ,last_non_direct_source.session_medium as session_medium_last_non_direct
    ,last_non_direct_source.session_source_category as session_source_category_last_non_direct
    ,last_non_direct_source.session_campaign as session_campaign_last_non_direct
    ,last_non_direct_source.session_content as session_content_last_non_direct
    ,last_non_direct_source.session_term as session_term_last_non_direct
    ,last_non_direct_source.session_default_channel_grouping as session_default_channel_grouping_last_non_direct
  from last_non_direct_session_partition_key
  left join {{ref('stg_ga4__sessions_traffic_sources_daily')}} last_non_direct_source on
    last_non_direct_session_partition_key.session_partition_key_last_non_direct = last_non_direct_source.session_partition_key

)

select * from join_last_non_direct_session_source

