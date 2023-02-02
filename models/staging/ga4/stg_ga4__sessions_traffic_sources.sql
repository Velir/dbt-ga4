with session_events as (
    select 
        session_key,
        event_timestamp,
        lower(source) as source,
        medium,
        campaign,
        source_category
    from {{ref('stg_ga4__attribution_window')}}
    left join {{ref('ga4_source_categories')}} using (source)
    --exclude the events session_start and first_visit because they are triggered first but never contain source, medium, campaign values
    where not ( event_name = "session_start" or event_name = "first_visit")
    and session_key is not null
   ),
set_default_channel_grouping as (
    select
        *,
        {{ga4.default_channel_grouping('source','medium','source_category')}} as default_channel_grouping
    from session_events
),
session_source as (
  select    
    session_key,
    ifnull( first_value( source ignore nulls ) over (partition by user_key order by unix_micros(timestamp_micros(event_timestamp)) range between 30 preceding and current row), '(direct)') as last_non_null_source,
    ifnull( first_value( medium ignore nulls ) over (partition by user_key order by unix_micros(timestamp_micros(event_timestamp)) range between 30 preceding and current row), '(none)') as last_non_null_medium
    ifnull( first_value( campaign ignore nulls ) over (partition by user_key order by unix_micros(timestamp_micros(event_timestamp)) range between 30 preceding and current row), '(direct)') as last_non_null_campaign
    ifnull( first_value( nullif(default_channel_grouping, 'Direct' ) ignore nulls ) over (partition by user_key order by unix_micros(timestamp_micros(event_timestamp)) range between 30 preceding and current row), 'Direct') as last_non_direct_channel_grouping
  from set_default_channel_grouping
)

select distinct  * from session_source