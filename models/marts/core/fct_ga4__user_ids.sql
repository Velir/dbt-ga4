with user_id_mapped as (
    select 
        client_keys.*,
        -- Use a user_id if it exists, otherwise fall back to the client_key
        coalesce(user_id_mapping.last_seen_user_id, client_keys.client_key) as user_id_or_client_key,
        -- Indicate whether the user_id_or_client_key value is a user_id
        CASE 
            WHEN user_id_mapping.last_seen_user_id is null THEN 0 ELSE 1
        END as is_user_id
    from {{ref('fct_ga4__client_keys')}} client_keys
    left join {{ref('stg_ga4__user_id_mapping')}} user_id_mapping using (client_key)
)

select
    user_id_or_client_key,
    stream_id,
    max(is_user_id) as is_user_id,
    min(first_seen_timestamp) as first_seen_timestamp,
    min(first_seen_start_date) as first_seen_start_date,
    sum(count_pageviews) as count_pageviews,
    sum(count_engaged_sessions) as count_engaged_sessions,
    sum(sum_event_value_in_usd) as sum_event_value_in_usd,
    sum(sum_engaged_time_msec) as sum_engaged_time_msec,
    sum(count_sessions) as count_sessions
    {% if var('conversion_events', false) %}
        {% for ce in var('conversion_events',[]) %}
            , sum(count_{{ce}}) as count_{{ce}}
        {% endfor %}
    {% endif %}
from user_id_mapped
group by 1, 2

