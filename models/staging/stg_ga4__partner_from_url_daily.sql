{% if var('static_incremental_days', false ) %}
    {% set partitions_to_replace = ['current_date'] %}
    {% for i in range(var('static_incremental_days')) %}
        {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            tags = ["incremental"],
            partition_by={
                "field": "session_partition_date",
                "data_type": "date",
                "granularity": "day"
            },
            partitions = partitions_to_replace
        )
    }}
{% else %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            tags = ["incremental"],
            partition_by={
                "field": "session_partition_date",
                "data_type": "date",
                "granularity": "day"
            }
        )
    }}
{% endif %}


with sessions_raw_from_events as
(
    select 
        session_partition_key
        ,event_date_dt as session_partition_date
        ,event_timestamp
				,event_key
    from {{ref('stg_ga4__events')}}
    where event_key is not null
    {% if is_incremental() %}
        {% if var('static_incremental_days', false ) %}
            and event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% else %}
            and event_date_dt >= _dbt_max_partition
        {% endif %}
    {% endif %}

),

 {{ ga4.partner_id_extract() }}
-- event_and_query_string as 
-- (
--     select 
--         event_key,
--         split(page_query_string, '&') as qs_split
--     from {{ref('stg_ga4__events')}}
-- ),
-- flattened_qs as
-- (
--     select 
--         event_key, 
--         params 
--     from event_and_query_string, unnest(qs_split) as params
-- ),
-- split_param_value as 
-- (
--     select 
--         event_key, 
--         split(params,'=')[SAFE_OFFSET(0)] as param, 
--     from flattened_qs
-- ),
-- add_parner_id as 
-- (
--     select 
--        *,
-- 				LTRIM(REGEXP_EXTRACT(param,r'^p[0-9]+'),'p') as partner_id
--     from split_param_value
-- )

SELECT DISTINCT
    session_partition_key
    ,session_partition_date
		, LAST_VALUE(partner_id IGNORE NULLS) OVER (session_window) AS partner_id
FROM sessions_raw_from_events
LEFT JOIN add_parner_id USING (event_key)
WINDOW session_window AS (PARTITION BY session_partition_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
