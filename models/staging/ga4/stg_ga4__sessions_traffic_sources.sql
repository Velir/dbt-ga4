with session_events as (
    select 
        session_key,
        user_key,
        event_timestamp,
        lower(source) as source,
        medium,
        campaign,
        content,
        term,
        source_category
    from {{ref('stg_ga4__attribution_window')}}
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
    select distinct
        session_key,
        user_key,
        -- if a user_id is added mid-session, the source gets populated as direct so we ignore direct and null source but defaults to direct/none
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' THEN source END) IGNORE NULLS) OVER (session_window), '(direct)') AS source, 
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(medium, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS medium,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(source_category, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS source_category,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(campaign, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS campaign,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(content, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS content,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(term, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS term,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(default_channel_grouping, 'Direct') END) IGNORE NULLS) OVER (session_window), 'Direct') AS default_channel_grouping,
        FIRST_VALUE((CASE WHEN source <> '(direct)' THEN event_timestamp END) IGNORE NULLS) OVER (session_window) AS event_timestamp,
    from set_default_channel_grouping
    WINDOW session_window AS (PARTITION BY session_key ORDER BY event_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
),
mv_custom as (
  select
        *,
        case
            when default_channel_grouping in ('Affiliates','Paid Search', 'Paid Video', 'Display', 'Cross-network', 'Paid Social', 'Paid Other', 'Paid Shopping', 'Audio','Email','Mobile Push Notifications', 'Other', 'SMS') then 'Paid'
            else 'Organic'
        end as mv_author_session_status,
  from session_source
)

select * from mv_custom