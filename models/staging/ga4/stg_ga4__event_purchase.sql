with purchase_with_params as (
    select * 
    FROM {{ref('stg_ga4__events')}},
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
    MAX(if(key = "page_referrer", value.string_value, NULL)) as page_referrer,
    MAX(if(key = "coupon", value.string_value, NULL)) as coupon,
    MAX(if(key = "transaction_id", value.string_value, NULL)) as transaction_id,
    MAX(if(key = "currency", value.string_value, NULL)) as currency,
    MAX(if(key = "payment_type", value.string_value, NULL)) as payment_type,
    MAX(if(key = "value", value.float_value, NULL)) as value
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