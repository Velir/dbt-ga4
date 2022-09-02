with page_views_by_user_key as (
    select distinct
        user_key,
        last_page_view_event_key
    from {{ref('stg_ga4__sessions_first_last_pageviews')}}
    where user_key is not null -- Remove users with privacy settings enabled
),
page_views_joined as (
    select
        page_views_by_user_key.*, 
        last_page_view.page_location as last_page_location,
        last_page_view.page_hostname as last_page_hostname,
        last_page_view.page_referrer as last_page_referrer
    from page_views_by_user_key
    left join {{ref('stg_ga4__event_page_view')}} last_page_view
        on page_views_by_user_key.last_page_view_event_key = last_page_view.event_key
)

select * from page_views_joined