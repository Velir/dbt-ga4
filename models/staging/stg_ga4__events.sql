-- This staging model contains key creation and window functions. Keeping window functions outside of the base incremental model ensures that the incremental updates don't artificially limit the window partition sizes (ex: if a session spans 2 days, but only 1 day is in the incremental update)

with base_events as (
    select * from {{ ref('base_ga4__events')}}
    {% if var('frequency', 'daily') == 'daily+streaming' %}
    union all
    select * from {{ref('base_ga4__events_intraday')}}
    {% endif %}
),
-- Add unique key for sessions. session_key will be null if user_pseudo_id is null due to consent being denied. ga_session_id may be null during audience trigger events. 
include_session_key as (
    select 
        *,
        to_base64(md5(CONCAT(stream_id, user_pseudo_id, CAST(session_id as STRING)))) as session_key -- Surrogate key to determine unique session across streams and users. Sessions do NOT reset after midnight in GA4
    from base_events
),
-- Add a key that combines session key and date. Useful when working with session table within date-partitioned tables
include_session_partition_key as (
    select 
        *,
        CONCAT(session_key, CAST(event_date_dt as STRING)) as session_partition_key
    from include_session_key
),
-- Add unique key for events
include_event_key as (
    select 
        *,
        to_base64(md5(CONCAT(session_key, event_name, CAST(event_timestamp as STRING), to_json_string(event_params)))) as event_key -- Surrogate key for unique events.  
    from include_session_partition_key
),
detect_gclid as (
    select
        * except (event_source, event_medium, event_campaign),
        case
            when (page_location like '%gclid%' and event_source is null) then "google"
            else event_source
        end as event_source,
        case
            when (page_location like '%gclid%' and event_medium is null) then "cpc"
            else event_medium
        end as event_medium,
        case
            when (page_location like '%gclid%' and event_campaign is null) then "(cpc)"
            else event_campaign
        end as event_campaign
    from include_event_key
),
-- Remove specific query strings from page_location field
remove_query_params as (

    select 
        * EXCEPT (page_location, page_referrer),
        page_location as original_page_location,
        page_referrer as original_page_referrer,
        {{ extract_page_path('page_location') }} as page_path,
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
),
page_key as (
    select
        *,
        (concat( cast(event_date_dt as string), page_location )) as page_key,
        case
            when event_name = 'page_view' then to_base64(md5(concat(session_key, page_referrer)))
            else to_base64(md5(concat(session_key, page_location)))
        end as page_engagement_key
    from enrich_params
)
select * from page_key