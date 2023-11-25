{{
    config(
        materialized='table'
    )
}}

with event_and_query_string as 
(
    select 
        event_key,
        split(page_query_string, '&') as qs_split
    from {{ref('stg_ga4__events')}}
    {% if is_incremental() %}
        where event_date_dt >= CURRENT_DATE() - 7
    {% endif %}
),
flattened_qs as
(
    select 
        event_key, 
        params 
    from event_and_query_string, unnest(qs_split) as params
),
split_param_value as 
(
    select 
        event_key, 
        split(params,'=')[SAFE_OFFSET(0)] as param, 
        NULLIF(split(params,'=')[SAFE_OFFSET(1)], '') as value 
    from flattened_qs
)

select * from split_param_value