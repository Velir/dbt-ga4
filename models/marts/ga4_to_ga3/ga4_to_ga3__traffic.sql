
-- Determine if the page was a bounced page by checking if it was the entrance page and if the session was a bounced session
with bounced_page_views as (
    select 
        page_views.event_key,
        CASE
            WHEN page_views.entrances > 0 and IFNULL(bounced_sessions.is_bounced_session, FALSE) THEN TRUE
            ELSE FALSE
        END AS is_bounced_page
    from {{ref('stg_ga4__event_page_view')}} page_views
        left join {{ref('stg_ga4__bounced_sessions')}} bounced_sessions
            on page_views.session_key = bounced_sessions.session_key

),
page_metrics_daily as (
    select 
        page_views.stream_id,
        page_views.event_date_dt as date,
        page_views.page_title,
        count(distinct page_views.session_key) as sessions,
        count(distinct page_views.client_id) as users,
        count(distinct page_views.client_id) as unique_pageviews,
        sum(value) as event_value,
        sum(page_views.entrances) as entrances,
        count(page_views.event_key) as pageviews,
        -- TODO: Unclear how to calculate time on page https://support.google.com/analytics/answer/9143382?hl=en#zippy=%2Cpage-screen%2Ctime%2Csession
        -- TODO: Unclear how to calculate exits
        sum(value) as page_value,
        sum(CAST(is_bounced_page as INT64))/count(page_views.event_key) as bounce_rate
    from {{ref('stg_ga4__event_page_view')}} page_views
    left join bounced_page_views
        on page_views.event_key = bounced_page_views.event_key
    group by 1,2,3
)

select * from page_metrics_daily
