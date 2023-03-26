select 
    session_partition_date as date_dt
    , geo_country
    , session_default_channel_grouping as default_channel_grouping
    , session_source as source
    , session_medium as medium
    , session_campaign as campaign
    , landing_page_path
    , landing_page_hostname
    , landing_page_referrer
    , device_category
    , session_last_page_path
    , sum(session_partition_max_session_engaged) as engaged_sessions
    , count(*) as sessions --assuming it's ok to treat multi-day session partitions as individual sessions
    , sum(session_partition_sum_engagement_time_msec) as sum_engagement_time_msec
    , sum(view_search_results_count) as sum_view_search_results
    , sum(session_partition_count_page_views) as sum_page_views
    , count(distinct user_pseudo_id) as users_daily
from {{ref('ga4', 'dim_ga4__sessions')}}
left join {{ref('ga4', 'fct_ga4__sessions_daily')}} fct_ga4__sessions_daily using (session_key)
group by 1,2,3,4,5,6,7,8,9,10,11