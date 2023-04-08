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

with session_events as (
    select 
        session_partition_key,
        event_date_dt as session_partition_date,
        event_timestamp,
        events.event_source,
        event_medium,
        event_campaign,
        event_content,
        event_term,
        source_category
    from {{ref('stg_ga4__events')}} events
    left join {{ref('ga4_source_categories')}} source_categories on events.event_source = source_categories.source
    where session_partition_key is not null
    and event_name != 'session_start'
    and event_name != 'first_visit'
    {% if is_incremental() %}
        {% if var('static_incremental_days', false ) %}
            and event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% else %}
            and event_date_dt >= _dbt_max_partition
        {% endif %}
    {% endif %}

   ),
set_default_channel_grouping as (
    select
        *,
        {{ga4.default_channel_grouping('event_source','event_medium','source_category')}} as default_channel_grouping
    from session_events
),
session_source as (
    select    
        session_partition_key,
        session_partition_date,
        COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN event_source END) IGNORE NULLS) OVER (session_window), '(direct)') AS session_source,
        COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_medium, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_medium,
        COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(source_category, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_source_category,
        COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_campaign, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_campaign,
        COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_content, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_content,
        COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_term, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_term,
        COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(default_channel_grouping, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_default_channel_grouping
    from set_default_channel_grouping
    WINDOW session_window AS (PARTITION BY session_partition_key ORDER BY event_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)

select distinct * from session_source