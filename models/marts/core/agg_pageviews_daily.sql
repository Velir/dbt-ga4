with source_sessions as (
    select 
        session_key
        , session_default_channel_grouping as default_channel_grouping
        , session_source as source
        , session_medium as medium
        , session_campaign as campaign
    from {{ref('ga4', 'dim_ga4__sessions')}}
)

select 
    page_views.event_date_dt as date_dt
    , page_views.pagepath_level_1 as site_section
    , page_views.geo_country
    , source_sessions.default_channel_grouping
    , source_sessions.source
    , source_sessions.medium
    , source_sessions.campaign
    , count(*) as pageviews 
    , count(distinct session_partition_key) as sessions
    , count(distinct user_pseudo_id) as users
    --TODO is_entrance
from {{ref('ga4', 'stg_ga4__event_page_view')}} page_views
left join source_sessions using (session_key)
group by 1,2,3,4,5,6,7