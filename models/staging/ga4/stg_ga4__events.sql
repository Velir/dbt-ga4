-- This staging model contains key creation and window functions. Keeping window functions outside of the base incremental model ensures that the incremental updates don't artificially limit the window partition sizes (ex: if a session spans 2 days, but only 1 day is in the incremental update)

with base_events as (
    select * from {{ ref('base_ga4__events')}}
    {% if var('frequency', 'daily') == 'daily+streaming' or  (var('include_intraday_events', false) == true and var('frequency', 'daily') != 'streaming' ) %}
    union all
    select * from {{ref('base_ga4__events_intraday')}}
    {% endif %}
),
-- Add a unique key for the user that checks for user_id and then pseudo_user_id
add_user_key as (
    select 
        *,
        case
            when user_id is not null then md5(user_id)
            when user_pseudo_id is not null then md5(user_pseudo_id)
            else null -- this case is reached when privacy settings are enabled
        end as user_key
    from base_events
), 
-- Add unique keys for sessions and events
include_session_key as (
    select 
        *,
        md5(CONCAT(stream_id, CAST(TO_BASE64(user_key) as STRING), cast(ga_session_id as STRING))) as session_key -- Surrogate key to determine unique session across streams and users. Sessions do NOT reset after midnight in GA4
    from add_user_key
),
include_event_number as (
    select include_session_key.*,
        row_number() over(partition by session_key) as session_event_number -- Number each event within a session to help generate a uniqu event key
    from include_session_key
),
include_event_key as (
    select 
        include_event_number.*,
        md5(CONCAT(CAST(TO_BASE64(session_key) as STRING), CAST(session_event_number as STRING))) as event_key -- Surrogate key for unique events
    from include_event_number
),
-- Remove specific query strings from page_location field
remove_query_params as (

    select 
        * EXCEPT (page_location),
        page_location as original_page_location,
        -- If there are query parameters to exclude, exclude them using regex
        {% if var('query_parameter_exclusions',none) is not none %}
        {{remove_query_parameters('page_location',var('query_parameter_exclusions'))}} as page_location
        {% else %}
        page_location
        {% endif %}
    from include_event_key
),
enrich_params as (
    select 
        *,
        {{extract_hostname_from_url('page_location')}} as page_hostname,
        {{extract_query_string_from_url('page_location')}} as page_query_string,
    from remove_query_params
)


select * from enrich_params