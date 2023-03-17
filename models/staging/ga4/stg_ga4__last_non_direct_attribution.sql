with last_non_direct as (
    select distinct
        first_value(session_key) over (user_window) as session_key,
        COALESCE(first_value((CASE WHEN session_source <> '(direct)' then session_source else null END) ignore nulls) OVER (user_window), '(direct)') AS last_non_direct_source,
        COALESCE(first_value((CASE WHEN session_source <> '(direct)' then session_medium else null END) ignore nulls) OVER (user_window), '(none)') AS last_non_direct_medium,
        COALESCE(first_value((CASE WHEN session_source <> '(direct)' then session_source_category else null END) ignore nulls) OVER (user_window), '(none)') AS last_non_direct_source_category,
        COALESCE(first_value((CASE WHEN session_source <> '(direct)' then session_campaign else null END) ignore nulls) OVER (user_window), '(none)') AS last_non_direct_campaign,
        COALESCE(first_value((CASE WHEN session_source <> '(direct)' then session_content else null END) ignore nulls) OVER (user_window), '(none)') AS last_non_direct_content,
        COALESCE(first_value((CASE WHEN session_source <> '(direct)' then session_term else null END) ignore nulls) OVER (user_window), '(none)') AS last_non_direct_term,
        COALESCE(first_value((CASE WHEN session_source <> '(direct)' then session_channel else null END) ignore nulls) OVER (user_window), 'Direct') AS last_non_direct_channel,
    from {{ ref('stg_ga4__sessions_traffic_sources') }}
    WINDOW user_window AS (PARTITION BY user_key ORDER BY session_start_timestamp desc  range between ({{var('attribution_window',30) * 24 * 60 * 60}}  )  preceding and current row)
)
select * from last_non_direct