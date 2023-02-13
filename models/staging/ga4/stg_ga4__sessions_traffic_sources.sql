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
last_non_direct as (
    select distinct
        session_key,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' and source is not null then source END)) OVER (session_window), '(direct)') AS last_non_direct_source,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' and source is not null then medium END)) OVER (session_window), '(none)') AS last_non_direct_medium,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' and source is not null then source_category END)) OVER (session_window), '(none)') AS last_non_direct_source_category,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' and source is not null then campaign END)) OVER (session_window), '(none)') AS last_non_direct_campaign,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' and source is not null then content END)) OVER (session_window), '(none)') AS last_non_direct_content,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' and source is not null then term END)) OVER (session_window), '(none)') AS last_non_direct_term,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' and source is not null then default_channel_grouping END)) OVER (session_window), 'Direct') AS last_non_direct_channel,
    from set_default_channel_grouping
    WINDOW session_window AS (PARTITION BY user_key ORDER BY unix_micros(timestamp_micros(event_timestamp)) range between ({{var('attribution_window',30) * 24 * 60 * 60}}  )  preceding and current row)
),
session_source as (
    select distinct
        session_key,
        -- if a user_id is added mid-session, the source gets populated as direct so we ignore direct and null source but defaults to direct/none
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' THEN source END) IGNORE NULLS) OVER (session_window), '(direct)') AS source, 
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(medium, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS medium,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(source_category, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS source_category,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(campaign, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS campaign,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(content, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS content,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(term, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS term,
        COALESCE(FIRST_VALUE((CASE WHEN source <> '(direct)' THEN COALESCE(default_channel_grouping, 'Direct') END) IGNORE NULLS) OVER (session_window), 'Direct') AS default_channel_grouping
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
  left join last_non_direct using (session_key)
)

select * from mv_custom