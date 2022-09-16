with page_views_first_last as (
    select
        session_key,
        user_key,
        FIRST_VALUE(event_timestamp) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_page_view_timestamp,
        FIRST_VALUE(event_date_dt) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_page_view_date_dt,
        FIRST_VALUE(event_key) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_page_view_event_key,
        LAST_VALUE(event_key) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_page_view_event_key,
    from {{ref('stg_ga4__events')}}
    where event_name = 'page_view'
),
events_by_session_key as (
    select distinct
        session_key,
        user_key,
        first_page_view_timestamp,
        first_page_view_date,
        first_page_view_event_key,
        last_page_view_event_key
    from page_views_first_last
)

select * from events_by_session_key