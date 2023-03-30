{% if is_incremental %}
    {% set partitions_to_replace = ['current_date'] %}
    {% for i in range(var('static_incremental_days', 1)) %}
        {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            schema = 'analytics',
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
            schema = 'analytics',
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
        )
    }}
{% endif %}
with ses as (
    select
        session_start_date as event_date_dt,
        mv_region,
        session_source,
        session_medium,
        sum(count_page_views) as page_views,
        sum(case when mv_author_session_status = 'Organic' then count_page_views end) as organic_page_views
    from {{ref('dim_ga4__sessions')}}
    {% if is_incremental() %}
        {% if var('static_incremental_days', 1 ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
    group by event_date_dt, mv_region, session_source, session_medium
)
select * from ses