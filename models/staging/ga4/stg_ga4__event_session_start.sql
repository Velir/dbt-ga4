with session_start_with_params as (
    select *
    FROM {{ref('stg_ga4__events')}} events,
        UNNEST(event_params) as params
    where event_name = 'session_start' 
        and (
            params.key = 'page_location' or 
            params.key = 'ga_session_number'
            )
),
pivoted as (
select 
    event_date_dt, 
    event_key,
    client_id, 
    user_id,
    event_timestamp, 
    event_name, 
    traffic_source_campaign_name,
    traffic_source_source,
    traffic_source_medium,
    session_key,
    MAX(if(key = "page_location", value.string_value, NULL)) as page_location,
    MAX(if(key = "ga_session_id", value.int_value, NULL)) as ga_session_id,
    MAX(if(key = "ga_session_number", value.int_value, NULL)) as ga_session_number,
from session_start_with_params
group by 1,2,3,4,5,6,7,8,9,10
)

select 
    *,
    {{extract_hostname_from_url('page_location')}} as page_hostname,
    case
        when ga_session_number = 1 then TRUE
        else FALSE
    end as is_new_user
from pivoted