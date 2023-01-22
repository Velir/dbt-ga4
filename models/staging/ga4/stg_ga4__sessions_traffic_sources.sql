with session_events as (
    select 
        session_key,
        event_timestamp,
        lower(source) as source,
        medium,
        campaign,
        content,
        term,
        source_category
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
        FIRST_VALUE((CASE WHEN source <> '(direct)' THEN source END) IGNORE NULLS) OVER (session_window) AS source,
        FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(medium, '(none)') END) IGNORE NULLS) OVER (session_window) AS medium,
        FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(source_category, '(none)') END) IGNORE NULLS) OVER (session_window) AS source_category,
        FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(campaign, '(none)') END) IGNORE NULLS) OVER (session_window) AS campaign,
        FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(content, '(none)') END) IGNORE NULLS) OVER (session_window) AS content,
        FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(term, '(none)') END) IGNORE NULLS) OVER (session_window) AS term,
        FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(default_channel_grouping, '(none)') END) IGNORE NULLS) OVER (session_window) AS default_channel_grouping
    from set_default_channel_grouping
    WINDOW session_window AS (PARTITION BY session_key ORDER BY event_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)

select distinct * from session_source