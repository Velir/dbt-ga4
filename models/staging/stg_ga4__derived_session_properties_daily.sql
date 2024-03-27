{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days')) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}
{{
    config(
        enabled = true if var('derived_session_properties', false) else false,
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


with unnest_event_params as
(
    select 
        session_partition_key
        ,event_date_dt as session_partition_date
        ,event_timestamp
        {% for sp in var('derived_session_properties', []) %}
            {% if sp.user_property %}
                , {{ ga4.unnest_key('user_properties', sp.user_property, sp.value_type) }}
            {% else %}
                , {{ ga4.unnest_key('event_params', sp.event_parameter, sp.value_type) }}
            {% endif %}
        {% endfor %}
    from {{ref('stg_ga4__events')}}
    where event_key is not null
    {% if is_incremental() %}
            and event_date_dt in ({{ partitions_to_replace | join(',') }})
    {% endif %}

)

SELECT DISTINCT
    session_partition_key
    ,session_partition_date
    {% for sp in var('derived_session_properties', []) %}
        , LAST_VALUE({{ sp.user_property | default(sp.event_parameter) }} IGNORE NULLS) OVER (session_window) AS {{ sp.session_property_name }}
    {% endfor %}
FROM unnest_event_params
WINDOW session_window AS (PARTITION BY session_partition_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
