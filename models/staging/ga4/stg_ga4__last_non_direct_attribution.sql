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
    from {{ ref('stg_ga4__sessions_traffic_sources') }}
    WINDOW session_window AS (PARTITION BY user_key ORDER BY unix_seconds(timestamp_seconds(event_timestamp)) range between ({{var('attribution_window',30) * 24 * 60 * 60}}  )  preceding and current row)
)
select * from last_non_direct