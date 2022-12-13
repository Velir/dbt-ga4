with session_events as (
    select 
        session_key,
        event_timestamp,
        lower(source) as source,
        medium,
        campaign,
        content,
        term,
        source_category,
        CASE WHEN ( event_name = "session_start" or event_name = "first_visit") THEN 0 ELSE 1 END AS weight
    from {{ref('stg_ga4__events')}}
    left join {{ref('ga4_source_categories')}} using (source)
    where session_key is not null
   ),
set_default_channel_grouping as (
    select
        *,
        {{ga4.default_channel_grouping('source','medium','source_category')}} as default_channel_grouping
    from session_events
),
session_source as (
    select    
        session_key,
        COALESCE(FIRST_VALUE(source) OVER (session_window), "(direct)") AS session_source,
        COALESCE(FIRST_VALUE(medium) OVER (session_window), "(none)") AS session_medium,
        COALESCE(FIRST_VALUE(campaign) OVER (session_window), "(none)") AS session_campaign,
        COALESCE(FIRST_VALUE(content) OVER (session_window), "(none)") AS session_content,
        COALESCE(FIRST_VALUE(term) OVER (session_window), "(none)") AS session_term,
        FIRST_VALUE(default_channel_grouping) OVER (session_window) AS session_default_channel_grouping
    from set_default_channel_grouping
    WINDOW session_window AS (PARTITION BY session_key ORDER BY weight DESC, event_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)

select distinct  * from session_source