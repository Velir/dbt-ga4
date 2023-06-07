-- This staging model contains key creation and window functions. Keeping window functions outside of the base incremental model ensures that the incremental updates don't artificially limit the window partition sizes (ex: if a session spans 2 days, but only 1 day is in the incremental update)
{% if not flags.FULL_REFRESH %}
    {% set partitions_to_query = ['current_date'] %}
    {% for i in range(var('static_incremental_days', 1)) %}
        {% set partitions_to_query = partitions_to_query.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
{% endif %}
with base_events as (
    select 
        *,
        {{ ga4.unnest_key('event_params', 'content_type') }} 
    from {{ ref('base_ga4__events')}}
    {% if not flags.FULL_REFRESH %}
        where event_date_dt in ({{ partitions_to_query | join(',') }})
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
        --case
        --    when user_id is not null then to_base64(md5(user_id))
        --    when user_pseudo_id is not null then to_base64(md5(user_pseudo_id))
        --    else null -- this case is reached when privacy settings are enabled
        --end as user_key
        to_base64(md5(user_pseudo_id)) as user_key
    from base_events
), 
-- Add unique keys for sessions and events
include_session_key as (
    select 
        *,
        to_base64(md5(CONCAT(stream_id, CAST(user_key as STRING), cast(ga_session_id as STRING)))) as session_key -- Surrogate key to determine unique session across streams and users. Sessions do NOT reset after midnight in GA4
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
        to_base64(md5(CONCAT(CAST(session_key as STRING), CAST(session_event_number as STRING)))) as event_key -- Surrogate key for unique events
    from include_event_number
),
include_page_key as (
    select
        include_event_key.*,
        to_base64(md5(concat( cast(event_date_dt as string), page_location ))) as page_key,
        case
            when event_name = 'page_view' then to_base64(md5(concat(session_key, page_referrer)))
            else to_base64(md5(concat(session_key, page_location)))
        end as page_engagement_key
    from include_event_key
),
detect_gclid as (
    select
        * except (medium, campaign),
        case
            when (page_location like '%gclid%' and source = 'jungroup') then "display"
            when (page_location like '%gclid%' and medium is null) then "cpc"
            else medium
        end as medium,
        case
            when (page_location like '%gclid%' and campaign is null) then "(cpc)"
            else campaign
        end as campaign
    from include_page_key
),
-- Remove specific query strings from page_location field
remove_query_params as (

    select 
        * EXCEPT (page_location, page_referrer),
        page_location as original_page_location,
        page_referrer as original_page_referrer,
        -- If there are query parameters to exclude, exclude them using regex
        {% if var('query_parameter_exclusions',none) is not none %}
        {{ga4.remove_query_parameters('page_location',var('query_parameter_exclusions'))}} as page_location,
        {{ga4.remove_query_parameters('page_referrer',var('query_parameter_exclusions'))}} as page_referrer,
        {% else %}
        page_location,
        page_referrer,
        {% endif %}
        case
            when engagement_time_msec >  1800000 then 0
            else engagement_time_msec
        end as engagement_time_msec
    from detect_gclid
),
enrich_params as (
    select 
        * except(event_name),
        {{ga4.extract_hostname_from_url('page_location')}} as page_hostname,
        {{ga4.extract_query_string_from_url('page_location')}} as page_query_string,
        case
            when geo_country = 'United States' then 'US'
            else 'Global'
        end as mv_region,
        case
            when event_name = 'page_view' and content_type = "piano_modal" then 'modal_page_view'
            when event_name = 'modal_pageview' then 'modal_page_view'
            else event_name
        end as event_name,

    from remove_query_params
)
select * except(content_type) from enrich_params