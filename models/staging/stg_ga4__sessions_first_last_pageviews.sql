{{
    config(
        materialized='table'
    )
}}

with page_views_first_last as (
    select
        stream_name,
        session_key,
        FIRST_VALUE(event_key) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_page_view_event_key,
        LAST_VALUE(event_key) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_page_view_event_key
    from {{ref('stg_ga4__events')}}
    where event_name = 'page_view'
    {% if is_incremental() %}
        and event_date_dt >= CURRENT_DATE() - 7
    {% endif %}
),
page_views_by_session_key as (
    select distinct
        stream_name,
        session_key,
        first_page_view_event_key,
        last_page_view_event_key
    from page_views_first_last
)

select * from page_views_by_session_key