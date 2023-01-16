{% if var('frequency', 'daily') == 'daily+streaming' %}
    {{ config(
    enabled = true 
    ) }}
{% else %}
    {{ config(
    enabled = false 
    ) }}
{% endif %}

-- This model will be unioned with `base_ga4__events` which means that their columns must match
with source as (
    select 
    {{ base_select_source() }}
    from {{ ref('base_ga4__multisite_intraday') }}
),
renamed as (
    select 
    {{ base_select_renamed() }}
    from source
)

select * from renamed
qualify row_number() over(partition by event_date_dt, stream_id, user_pseudo_id, ga_session_id, event_name, event_timestamp, to_json_string(event_params)) = 1
