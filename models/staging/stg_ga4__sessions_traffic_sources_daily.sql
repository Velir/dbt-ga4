{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days')) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}


{% set start_date = var('start_date', none) %}
{% set end_date = var('end_date', none) %}

{{ log("Initial start_date: " ~ start_date, info=True) }}
{{ log("Initial end_date: " ~ end_date, info=True) }}


{% if start_date and end_date %}
    {{ log("Running with start_date: " ~ start_date, info=True) }}
    {{ log("Running with end_date: " ~ end_date, info=True) }}

    {% set formatted_start_date = start_date[:4] ~ '-' ~ start_date[4:6] ~ '-' ~ start_date[6:] %}
    {% set formatted_end_date = end_date[:4] ~ '-' ~ end_date[4:6] ~ '-' ~ end_date[6:] %}

    {{ log("Formatted start_date: " ~ formatted_start_date, info=True) }}
    {{ log("Formatted end_date: " ~ formatted_end_date, info=True) }}

    {% set date_array = generate_date_array(start_date, end_date) %}

    
    {% set partitions_to_replace = [] %}
    {% for date in date_array %}
        {% set formatted_date = "date('" ~ date ~ "')" %}
        {% do partitions_to_replace.append(formatted_date) %}
    {% endfor %}

{% endif %}

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


with session_events as (
    select
        stream_id
        ,client_key
        ,session_partition_key
        ,event_date_dt as session_partition_date
        ,event_timestamp
        ,events.event_source
        ,event_medium
        ,event_campaign
        ,event_content
        ,event_term
        ,source_category
    from {{ref('stg_ga4__events')}} events
    left join {{ref('ga4_source_categories')}} source_categories on events.event_source = source_categories.source
    where session_partition_key is not null
    and event_name != 'session_start'
    and event_name != 'first_visit'
    {% if is_incremental() %}
            and event_date_dt in ({{ partitions_to_replace | join(',') }})
    {% endif %}

   ),
set_default_channel_grouping as (
    select
        *
        ,{{ga4.default_channel_grouping('event_source','event_medium','source_category', 'event_campaign')}} as default_channel_grouping
    from session_events
),
first_session_source as (
    select
        stream_id
        ,client_key
        ,session_partition_key
        ,session_partition_date
        ,event_timestamp
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN event_source END) IGNORE NULLS) OVER (session_window), '(direct)') AS session_source
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_medium, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_medium
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(source_category, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_source_category
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_campaign, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_campaign
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_content, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_content
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(event_term, '(none)') END) IGNORE NULLS) OVER (session_window), '(none)') AS session_term
        ,COALESCE(FIRST_VALUE((CASE WHEN event_source <> '(direct)' THEN COALESCE(default_channel_grouping, 'Direct') END) IGNORE NULLS) OVER (session_window), 'Direct') AS session_default_channel_grouping
    from set_default_channel_grouping
    WINDOW session_window AS (PARTITION BY session_partition_key ORDER BY event_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
),
find_non_direct_session_partition_key as (

    select
        *
        ,if(session_source <> '(direct)', session_partition_key, null) as non_direct_session_partition_key --provide the session_partition_key only if source is not direct. Useful for last non-direct attribution modeling
    from first_session_source
)

select  stream_id
        ,client_key
        ,session_partition_key
        ,session_partition_date
        ,session_source
        ,session_medium
        ,session_source_category
        ,session_campaign
        ,session_content
        ,session_term
        ,session_default_channel_grouping
        ,non_direct_session_partition_key
        ,min(event_timestamp) as session_partition_timestamp
from find_non_direct_session_partition_key
group by all