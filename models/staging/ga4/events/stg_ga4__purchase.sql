with purchase_with_params as (
    select 
        event_date_dt, 
        user_id,
        user_pseudo_id, 
        event_timestamp, 
        event_name, params, 
        traffic_source 
    FROM {{ref('base_ga4__events')}},
        UNNEST(event_params) as params
    where event_name = 'purchase' -- Pull only 'purchase' events 
        and 
        (
            params.key = 'page_location' or 
            params.key = 'ga_session_id' or 
            params.key = 'ga_session_number' or
            params.key = 'page_referrer' or
            params.key = 'currency' or
            params.key = 'value' or 
            params.key = 'payment_type' or 
            params.key = 'coupon' or 
            params.key = 'transaction_id'
        )
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
    MAX(if(params.key = "page_referrer", params.value.string_value, NULL)) as page_referrer,
    MAX(if(params.key = "coupon", params.value.string_value, NULL)) as coupon,
    MAX(if(params.key = "transaction_id", params.value.string_value, NULL)) as transaction_id,
    MAX(if(params.key = "currency", params.value.string_value, NULL)) as currency,
    MAX(if(params.key = "payment_type", params.value.string_value, NULL)) as payment_type,
    MAX(if(params.key = "value", params.value.float_value, NULL)) as value
    -- TODO how to handle items array?
    
from purchase_with_params
group by 1,2,3,4,5,6,7,8
)

select 
    *,
    case
        when ga_session_number = 1 then TRUE
        else FALSE
    end as is_new_user
from pivoted