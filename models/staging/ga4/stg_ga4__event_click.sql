with click_with_params as (
    select * 
    FROM {{ref('stg_ga4__events')}},
        UNNEST(event_params) as params
    where event_name = 'click' 
        and 
        (
            params.key = 'page_location' or 
            params.key = 'ga_session_number' or
            params.key = 'entrances' or 
            params.key = 'page_title' or 
            params.key = 'page_referrer' or
            params.key = 'value' or
            params.key = 'outbound' or
            params.key = 'link_domain' or
            params.key = 'link_url' or
            params.key = 'click_element' or 
            params.key = 'click_id' or
            params.key = 'click_region' or
            params.key = 'click_tag_name' or 
            params.key = 'click_url' or 
            params.key = 'file_name'
        )
),
pivoted as (
select 
    stream_id,
    event_key,
    event_date_dt, 
    client_id, 
    user_id,
    event_timestamp, 
    event_name, 
    traffic_source_campaign_name,
    traffic_source_source,
    traffic_source_medium,
    session_key,
    MAX(if(key = "page_location", value.string_value, NULL)) as page_location,
    MAX(if(key = "ga_session_number", value.int_value, NULL)) as ga_session_number,
    MAX(if(key = "entrances", value.int_value, 0)) as entrances,
    MAX(if(key = "page_title", value.string_value, NULL)) as page_title,
    MAX(if(key = "page_referrer", value.string_value, NULL)) as page_referrer,
    MAX(if(key = "value", value.float_value, NULL)) as value,
    MAX(if(key = "outbound", value.string_value, NULL)) as outbound,
    MAX(if(key = "link_domain", value.string_value, NULL)) as link_domain,
    MAX(if(key = "link_url", value.string_value, NULL)) as link_url,
    MAX(if(key = "click_element", value.string_value, NULL)) as click_element,
    MAX(if(key = "click_id", value.string_value, NULL)) as click_id,
    MAX(if(key = "click_region", value.string_value, NULL)) as click_region,
    MAX(if(key = "click_tag_name", value.string_value, NULL)) as click_tag_name,
    MAX(if(key = "click_url", value.string_value, NULL)) as click_url,
    MAX(if(key = "file_name", value.string_value, NULL)) as file_name
    
from click_with_params
group by 1,2,3,4,5,6,7,8,9,10,11
)

select 
    *,
    {{extract_hostname_from_url('page_location')}} as page_hostname,
    case
        when ga_session_number = 1 then TRUE
        else FALSE
    end as is_new_user
from pivoted