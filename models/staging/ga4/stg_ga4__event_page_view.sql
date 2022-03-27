with page_view_with_params as (
    select * 
    FROM {{ref('stg_ga4__events')}},
        UNNEST(event_params) as params
    where event_name = 'page_view' 
        and 
        (
            params.key = 'page_location' or 
            params.key = 'ga_session_id' or 
            params.key = 'ga_session_number' or
            params.key = 'entrances' or 
            params.key = 'page_title' or 
            params.key = 'page_referrer' or
            params.key = 'value'
        )
),
pivoted as (
select 
    stream_id,
    event_date_dt, 
    client_id, 
    user_id,
    event_timestamp, 
    event_name, 
    traffic_source_campaign_name,
    traffic_source_source,
    traffic_source_medium,
    MAX(if(key = "page_location", value.string_value, NULL)) as page_location,
    MAX(if(key = "ga_session_id", value.int_value, NULL)) as ga_session_id,
    MAX(if(key = "ga_session_number", value.int_value, NULL)) as ga_session_number,
    MAX(if(key = "entrances", value.int_value, 0)) as entrances,
    MAX(if(key = "page_title", value.string_value, NULL)) as page_title,
    MAX(if(key = "page_referrer", value.string_value, NULL)) as page_referrer,
    MAX(if(key = "value", value.float_value, NULL)) as value
    
from page_view_with_params
group by 1,2,3,4,5,6,7,8,9
)

select 
    *,
    {{extract_hostname_from_url('page_location')}} as page_hostname,
    case
        when ga_session_number = 1 then TRUE
        else FALSE
    end as is_new_user
from pivoted