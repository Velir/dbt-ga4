with session_events as (
    select 
        session_key,
        event_timestamp,
        lower(source) as source,
        medium,
        campaign,
        source_category
    from {{ref('stg_ga4__events')}}
    left join {{ref('ga4_source_categories')}} using (source)
    --exclude the events session_start and first_visit because they are triggered first but never contain source, medium, campaign values
    where not ( event_name = "session_start" or event_name = "first_visit")
    and session_key is not null
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

        (case
          when FIRST_VALUE(source) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) is null then "(direct)"
          else FIRST_VALUE(source) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) end
        ) as session_source,

        (case 
          when FIRST_VALUE(medium) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) is null then "(none)"
          else FIRST_VALUE(medium) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) end
        ) as session_medium,

        (case 
          when FIRST_VALUE(campaign) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) is null then "(direct)"
          else FIRST_VALUE(campaign) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) end
        ) as session_campaign,

        FIRST_VALUE(default_channel_grouping) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS session_default_channel_grouping
    from set_default_channel_grouping
)

select distinct  * from session_source