-- Mart for dimensions related to user devices (based on client_key)

with include_first_last_events as (
    select 
        *
    from {{ref('stg_ga4__client_key_first_last_events')}}
),
include_first_last_page_views as (
    select 
        include_first_last_events.*,
        first_last_page_views.first_page_location,
        first_last_page_views.first_page_hostname,
        first_last_page_views.first_page_referrer,
        first_last_page_views.last_page_location,
        first_last_page_views.last_page_hostname,
        first_last_page_views.last_page_referrer
    from include_first_last_events 
    left join {{ref('stg_ga4__client_key_first_last_pageviews')}} as first_last_page_views using (client_key)
),
include_user_properties as (
    

select * from include_first_last_page_views
{% if var('derived_user_properties', false) %}
-- If derived user properties have been assigned as variables, join them on the client_key
left join {{ref('stg_ga4__derived_user_properties')}} using (client_key)
{% endif %}
{% if var('user_properties', false) %}
-- If user properties have been assigned as variables, join them on the client_key
left join {{ref('stg_ga4__user_properties')}} using (client_key)
{% endif %}

)

select * from include_user_properties