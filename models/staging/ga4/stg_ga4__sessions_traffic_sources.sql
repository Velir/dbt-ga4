-- TODO this isn't quite right because sessions are appearing with null source/medium/dfc when they should be direct

with session_events as (
    select 
        session_key,
        event_timestamp,
        lower(source) as source,
        medium,
        source_category
    from {{ref('stg_ga4__events')}}
    left join {{ref('ga4_source_categories')}} using (source)
    where source is not null and medium is not null
   ),
set_default_channel_grouping as (
    select
        *,
        {{default_channel_grouping('source','medium','source_category')}} as default_channel_grouping
    from session_events
),
session_source as (
    select    
        session_key,
        FIRST_VALUE(source) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS session_source,
        FIRST_VALUE(medium) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS session_medium,
        FIRST_VALUE(default_channel_grouping) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS session_default_channel_grouping
    from set_default_channel_grouping
)

select distinct  * from session_source