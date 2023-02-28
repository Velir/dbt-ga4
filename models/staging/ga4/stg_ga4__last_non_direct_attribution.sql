with last_non_direct as (
    select distinct
        session_key,
        COALESCE(LAST_VALUE((CASE WHEN source <> '(direct)' and source is not null then source END)) OVER (user_window), '(direct)') AS last_non_direct_source,
        COALESCE(LAST_VALUE((CASE WHEN source <> '(direct)' and source is not null then medium END)) OVER (user_window), '(none)') AS last_non_direct_medium,
        COALESCE(LAST_VALUE((CASE WHEN source <> '(direct)' and source is not null then source_category END)) OVER (user_window), '(none)') AS last_non_direct_source_category,
        COALESCE(LAST_VALUE((CASE WHEN source <> '(direct)' and source is not null then campaign END)) OVER (user_window), '(none)') AS last_non_direct_campaign,
        COALESCE(LAST_VALUE((CASE WHEN source <> '(direct)' and source is not null then content END)) OVER (user_window), '(none)') AS last_non_direct_content,
        COALESCE(LAST_VALUE((CASE WHEN source <> '(direct)' and source is not null then term END)) OVER (user_window), '(none)') AS last_non_direct_term,
        COALESCE(LAST_VALUE((CASE WHEN source <> '(direct)' and source is not null then default_channel_grouping END)) OVER (user_window), 'Direct') AS last_non_direct_channel,
    from {{ ref('stg_ga4__sessions_traffic_sources') }}
    WINDOW user_window AS (PARTITION BY user_key ORDER BY event_timestamp  range between ({{var('attribution_window',30) * 24 * 60 * 60}}  )  preceding and current row)
)
select * from last_non_direct