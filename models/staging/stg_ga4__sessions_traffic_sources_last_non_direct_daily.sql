with assign_null as (
  select
    client_key
    ,session_partition_key
    ,session_partition_date
    ,case when session_default_channel_grouping = '(none)' then null else session_default_channel_grouping end as session_default_channel_grouping
  from {{ref('stg_ga4__sessions_traffic_sources_daily')}}
)

select
  client_key
  ,session_partition_key
  ,session_partition_date
  ,session_default_channel_grouping
  ,CASE
    WHEN session_default_channel_grouping is null
    THEN 
        last_value(session_default_channel_grouping ignore nulls) over(
        partition by client_key
        order by
            UNIX_MICROS(TIMESTAMP(session_partition_date)) range between 2592000000000 preceding
            and current row -- 30 day lookback
        )
    ELSE session_default_channel_grouping
  END as session_default_channel_grouping_last_non_direct,
from
  assign_null