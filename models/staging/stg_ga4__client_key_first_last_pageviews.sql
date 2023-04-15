{{
    config(materialized = "table")
}}

with page_views_first_last as (
    select
        client_key,
        FIRST_VALUE(event_key) OVER (PARTITION BY client_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_page_view_event_key,
        LAST_VALUE(event_key) OVER (PARTITION BY client_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_page_view_event_key
    from {{ref('stg_ga4__event_page_view')}}
    where client_key is not null -- Remove users with privacy settings enabled
),
page_views_by_client_key as (
    select distinct
        client_key,
        first_page_view_event_key,
        last_page_view_event_key
    from page_views_first_last
),

page_views_joined as (
    select
        page_views_by_client_key.*,
        first_page_view.page_location as first_page_location,
        first_page_view.page_hostname as first_page_hostname,
        first_page_view.page_referrer as first_page_referrer, 
        last_page_view.page_location as last_page_location,
        last_page_view.page_hostname as last_page_hostname,
        last_page_view.page_referrer as last_page_referrer
    from page_views_by_client_key
    left join {{ref('stg_ga4__event_page_view')}} first_page_view
        on page_views_by_client_key.first_page_view_event_key = first_page_view.event_key
    left join {{ref('stg_ga4__event_page_view')}} last_page_view
        on page_views_by_client_key.last_page_view_event_key = last_page_view.event_key
)

select * from page_views_joined