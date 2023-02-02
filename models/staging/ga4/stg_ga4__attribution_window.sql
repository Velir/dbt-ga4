-- This staging model contains key creation and window functions. Keeping window functions outside of the base incremental model ensures that the incremental updates don't artificially limit the window partition sizes (ex: if a session spans 2 days, but only 1 day is in the incremental update)
{% if not flags.FULL_REFRESH %}
    {% set partitions_to_query = ['current_date'] %}
    {% for i in range(var('attribution_window', 30)) %}
        {% set partitions_to_query = partitions_to_query.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
{% endif %}
with base_events as (
    select
        user_pseudo_id,
        event_date_dt,
        event_timestamp,
        campaign,
        source,
        medium,
        event_name,
        stream_id,
        ga_session_id
    from {{ ref('base_ga4__events')}}
    where (source is not null and medium is not null and campaign is not null)
    {% if not flags.FULL_REFRESH %}
        and event_date_dt in ({{ partitions_to_query | join(',') }})
    {% endif %}
    {% if var('frequency', 'daily') == 'daily+streaming' %}
    union all
    select * from {{ref('base_ga4__events_intraday')}}
    {% endif %}
),
-- Add a unique key for the user that checks for user_id and then pseudo_user_id
add_user_key as (
    select 
        *,
        to_base64(md5(user_pseudo_id)) as user_key
        -- in this implementation, sessions break when a user id is added or removed during a session
        -- there is a fix in the main package, but implementing it here is a major task
        --case
        --    when user_id is not null then to_base64(md5(user_id))
        --    when user_pseudo_id is not null then to_base64(md5(user_pseudo_id))
        --    else null -- this case is reached when privacy settings are enabled
        --end as user_key
    from base_events
),
include_session_key as (
    select 
        *,
        to_base64(md5(CONCAT(stream_id, CAST(user_key as STRING), cast(ga_session_id as STRING)))) as session_key -- Surrogate key to determine unique session across streams and users. Sessions do NOT reset after midnight in GA4
    from add_user_key
)


select * from include_session_key