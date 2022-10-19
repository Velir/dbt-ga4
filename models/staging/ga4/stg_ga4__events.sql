-- This staging model contains key creation and window functions. Keeping window functions outside of the base incremental model ensures that the incremental updates don't artificially limit the window partition sizes (ex: if a session spans 2 days, but only 1 day is in the incremental update)

with base_events as (
    select * from {{ ref('base_ga4__events')}}
    {% if var('frequency', 'daily') == 'daily+streaming' %}
    union all
    select * from {{ref('base_ga4__events_intraday')}}
    {% endif %}
),
-- Add a unique key for the user that checks for user_id and then pseudo_user_id
add_user_key as (
    select 
        *,
        case
            when user_id is not null then to_base64(md5(user_id))
            when user_pseudo_id is not null then to_base64(md5(user_pseudo_id))
            else null -- this case is reached when privacy settings are enabled
        end as user_key
    from base_events
), 
-- Add unique keys for sessions and events
include_session_key as (
    select 
        *,
        to_base64(md5(CONCAT(stream_id, user_pseudo_id, ga_session_id))) as session_key -- Surrogate key to determine unique session across streams and users. Sessions do NOT reset after midnight in GA4
    from add_user_key
),
/*
-- Removing row_number() calculation as it negates the base table partition. Would like to include equivalent method of determining event uniqueness somehow
include_event_number as (
    select include_session_key.*,
        row_number() over(partition by session_key) as session_event_number -- Number each event within a session to help generate a uniqu event key
    from include_session_key
),*/
include_event_key as (
    select 
        include_session_key.*,
        to_base64(md5(CONCAT(CAST(session_key as STRING), event_name, CAST(event_timestamp as STRING)))) as event_key -- Surrogate key for unique events
    from include_session_key
),
detect_gclid as (
    select
        * except (medium, campaign),
        case
            when page_location like '%gclid%' then "cpc"
            else medium
        end as medium,
        case
            when page_location like '%gclid%' then "(cpc)"
            else campaign
        end as campaign
    from include_event_key
),
-- Remove specific query strings from page_location field
remove_query_params as (

    select 
        * EXCEPT (page_location, page_referrer),
        page_location as original_page_location,
        page_referrer as original_page_referrer,
        -- If there are query parameters to exclude, exclude them using regex
        {% if var('query_parameter_exclusions',none) is not none %}
        {{remove_query_parameters('page_location',var('query_parameter_exclusions'))}} as page_location,
        {{remove_query_parameters('page_referrer',var('query_parameter_exclusions'))}} as page_referrer
        {% else %}
        page_location,
        page_referrer
        {% endif %}
    from detect_gclid
),
enrich_params as (
    select 
        *,
        {{extract_hostname_from_url('page_location')}} as page_hostname,
        {{extract_query_string_from_url('page_location')}} as page_query_string,
    from remove_query_params
)
select * from enrich_params