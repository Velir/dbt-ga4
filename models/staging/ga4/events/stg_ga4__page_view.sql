with page_view_with_params as (
    select 
        event_date_dt, 
        user_id,
        user_pseudo_id, 
        event_timestamp, 
        event_name, params, 
        traffic_source 
    FROM {{ref('base_ga4__events')}},
        UNNEST(event_params) as params
    where event_name = 'page_view' 
        and (params.key = 'page_location' or 
        params.key = 'ga_session_id' or 
        params.key = 'ga_session_number' or
        params.key = 'entrances' or 
        params.key = 'page_title' or 
        params.key = 'page_referrer')
),
pivoted as (
select 
    event_date_dt, 
    user_pseudo_id, 
    user_id,
    event_timestamp, 
    event_name, 
    traffic_source.name as traffic_source_name,
    traffic_source.source as traffic_source_source,
    traffic_source.medium as traffic_source_medium,
    MAX(if(params.key = "page_location", params.value.string_value, NULL)) as page_location,
    MAX(if(params.key = "ga_session_id", params.value.int_value, NULL)) as ga_session_id,
    MAX(if(params.key = "ga_session_number", params.value.int_value, NULL)) as ga_session_number,
    MAX(if(params.key = "entrances", params.value.int_value, 0)) as entrances,
    MAX(if(params.key = "page_title", params.value.string_value, NULL)) as page_title,
    MAX(if(params.key = "page_referrer", params.value.string_value, NULL)) as page_referrer
    
from page_view_with_params
group by 1,2,3,4,5,6,7,8
)

select 
    *,
    case
        when ga_session_number = 1 then TRUE
        else FALSE
    end as is_new_user
from pivoted