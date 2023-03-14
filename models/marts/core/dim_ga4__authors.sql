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
with authors as (
    select
        author,
        event_date_dt,
        concat(cast(extract(year from event_date_dt) as string),'-',  cast(extract(month from event_date_dt) as string)) as year_month,
        count(distinct case when date_trunc(event_date_dt, month) = date_trunc(article_pubdate, month) then page_location else null end) as articles_published_in_year_month,
        count (geo_country) as page_views,
        countif (geo_country = 'United States' and mv_author_session_status = 'author_payable') as us_organic_page_views,
        countif (geo_country = 'United States' and mv_author_session_status = 'author_non_payable') as us_paid_page_views,
        countif (geo_country != 'United States' and mv_author_session_status = 'author_payable') as global_organic_page_views,
        countif (geo_country != 'United States' and mv_author_session_status = 'author_non_payable') as global_paid_page_views,
    from {{ref('fct_ga4__event_page_view')}}
    {% if is_incremental() and var('static_incremental_days', 1) %}
        where event_date_dt in ({{ partitions_to_replace | join(',') }})
    {% endif %}
    group by 1,2,3
)
select * from authors