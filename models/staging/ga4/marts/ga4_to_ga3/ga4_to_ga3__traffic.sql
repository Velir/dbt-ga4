--TODO replace with bounced_sessions model
with pageviews_per_session as (
    select 
        stream_id, 
        event_date_dt,
        ga_session_id,
        count(page_title) as pageviews
    from {{ref('stg_ga4__event_page_view')}}
    group by 1,2,3
),
bounce_count_daily as (
    select 
        stream_id, 
        event_date_dt,
        count(ga_session_id) as bounces
    from pageviews_per_session
    where pageviews = 1
    group by 1,2
),
metrics as (
    select 
        page_views.stream_id,
        page_views.event_date_dt as date,
        page_views.page_title,
        count(distinct page_views.ga_session_id) as sessions,
        count(distinct page_views.client_id) as users,
        count(distinct page_views.client_id) as unique_pageviews,
        sum(value) as event_value,
        sum(entrances) as entrances,
        count(page_title) as pageviews,
        -- TODO: Unclear how to calculate time on page https://support.google.com/analytics/answer/9143382?hl=en#zippy=%2Cpage-screen%2Ctime%2Csession
        -- TODO: Unclear how to calculate exits
        sum(value) as page_value,
        sum(bounce_count_daily.bounces) as bounces
    from {{ref('stg_ga4__event_page_view')}} page_views
    left join bounce_count_daily 
        on page_views.stream_id = bounce_count_daily.stream_id 
            and page_views.event_date_dt = bounce_count_daily.event_date_dt
    group by 1,2,3
)

select *, bounces/sessions as bounce_rate from metrics