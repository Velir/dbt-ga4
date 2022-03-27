
with bounced_pages as (
    select 
        page_title,
        session_key,
        entrances,
        IFNULL(is_bounced_session, FALSE) as is_bounced_session
        CASE
            WHEN entrances > 0 and IFNULL(is_bounced_session, FALSE) THEN TRUE
            ELSE FALSE
        END AS is_bounced_page
    from {{ref('stg_ga4__event_page_view')}} page_views
        left join {{ref('stg_ga4__bounced_sessions')}} bounced_sessions
            on page_views.session_key = bounced_sessions.session_key

),

traffic_metrics_daily as (
    select 
        page_views.stream_id,
        page_views.event_date_dt as date,
        page_views.page_title,
        CASE
            WHEN IFNULL(bounced_sessions.is_bounced_session, FALSE) and sum(entrances) > 0 THEN TRUE
            ELSE FALSE
        END AS is_bounced_page,
        count(distinct page_views.session_key) as sessions,
        count(distinct page_views.client_id) as users,
        count(distinct page_views.client_id) as unique_pageviews,
        sum(value) as event_value,
        sum(entrances) as entrances,
        count(page_title) as pageviews,
        -- TODO: Unclear how to calculate time on page https://support.google.com/analytics/answer/9143382?hl=en#zippy=%2Cpage-screen%2Ctime%2Csession
        -- TODO: Unclear how to calculate exits
        sum(value) as page_value
        
    from {{ref('stg_ga4__event_page_view')}} page_views
    left join {{ref('stg_ga4__bounced_sessions')}} bounced_sessions
        on page_views.session_key = bounced_sessions.session_key
    group by 1,2,3,4,5
),
calculate_bounced_rate as (
    select
        
    from traffic_metrics
)
