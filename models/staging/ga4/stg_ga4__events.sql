-- This staging model contains key creation and window functions. Keeping window functions outside of the base incremental model ensures that the incremental updates don't artificially limit the window partition sizes (ex: if a session spans 2 days, but only 1 day is in the incremental update)

with base_events as (
    select * from {{ ref('base_ga4__events')}}
    {% if var('include_intraday_events', false) %}
    union all
    select * from {{ref('base_ga4__events_intraday')}}
    {% endif %}
),
-- Add unique keys for sessions and events
include_session_key as (
    select 
        base_events.*,
        md5(CONCAT(stream_id, client_id, cast(ga_session_id as STRING))) as session_key -- Surrogate key to determine unique session across streams and users. Sessions do NOT reset after midnight in GA4
    from base_events
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
url_enrichment as (
    select 
        include_event_key.*,
        {{extract_hostname_from_url('page_location')}} as page_hostname,
        {{extract_query_string_from_url('page_location')}} as page_query_string,
    from include_event_key
)


select * from url_enrichment