with page_views_by_user_key as (
    select distinct
        user_key,
        last_page_view_event_key
    from
    (
        select
            user_key,
            last_page_view_event_key,
            row_number() over (partition by user_key order by first_event_timestamp desc) as rn 
        from {{ref('stg_ga4__sessions_first_last_pageviews')}} 
    ) 
    where rn = 1
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

select distinct * from page_views_joined