{% if is_incremental %}
    {% set partitions_to_replace = ['current_date'] %}
    {% for i in range(var('static_incremental_days', 1)) %}
        {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
            partitions = partitions_to_replace,
        )
    }}
{% else %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
        )
    }}
{% endif %}
with page_view as (
    select
        page_key,
        event_date_dt,
        page_location,  -- includes query string parameters not listed in query_parameter_exclusions variable
        last_page_title_page_key
        {% if var("dim_ga4__pages_custom_parameters", "none") != "none" %}  
            {{ ga4.mart_custom_parameters( var("dim_ga4__pages_custom_parameters") )}} 
        {% endif %}
    from (
        select
            page_key,
            event_date_dt,
            page_location,
            last_value(page_title) OVER (PARTITION BY page_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_page_title_page_key
            {% if var("dim_ga4__pages_custom_parameters", "none") != "none" %}  
                {{ ga4.mart_custom_parameters( var("dim_ga4__pages_custom_parameters") )}} 
            {% endif %}
        from {{ref('stg_ga4__event_page_view')}}
    )
    {% if is_incremental() %}
    {% if var('static_incremental_days', 1 ) %}
        where event_date_dt in ({{ partitions_to_replace | join(',') }})
    {% endif %}
{% endif %}
    group by 1,2,3,4
    {% if var("dim_ga4__pages_custom_parameters", "none") != "none" %}  
        {{ ga4.mart_group_by_custom_parameters( var("dim_ga4__pages_custom_parameters") )}} 
    {% endif %}
)
select
    *
from page_view