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
        enabled= var('conversion_events', false) != false,
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        tags = ["incremental"],
        partition_by={
            "field": "session_partition_date",
            "data_type": "date",
            "granularity": "day"
        },
        partitions = partitions_to_replace,
        cluster_by = ['stream_id']
    )
}}



with event_counts as (
    select 
        stream_id,
        session_key,
        session_partition_key,
        min(event_date_dt) as session_partition_date -- The date of this session partition
        {% for ce in var('conversion_events',[]) %}
        , countif(event_name = '{{ce}}') as {{ga4.valid_column_name(ce)}}_count
        {% endfor %}
    from {{ref('stg_ga4__events')}}
    where 1=1
    {% if is_incremental() %}
            and event_date_dt in ({{ partitions_to_replace | join(',') }})
    {% endif %}
    group by all
)

select * from event_counts
