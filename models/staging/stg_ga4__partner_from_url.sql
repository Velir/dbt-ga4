{{ config(
  materialized = "table"
) }}

-- Remove null session_keys (users with privacy enabled)
with events_from_valid_users as (
    select * from {{ref('stg_ga4__events')}}
    where session_key is not null
),
sessions_raw_from_events as
(
    select 
        session_key,
        event_timestamp,
				event_key
    from events_from_valid_users
),
{{ ga4.partner_id_extract() }}

SELECT DISTINCT
    session_key
    , LAST_VALUE(partner_id IGNORE NULLS) OVER (session_window) AS partner_id
FROM sessions_raw_from_events
LEFT JOIN add_parner_id USING (event_key)
WINDOW session_window AS (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
