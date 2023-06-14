-- not a partitioned model, but using partitions to replace for query pruning
{% set partitions_to_replace = ["current_date"] %}
{% for i in range(var("static_incremental_days", 1)) %}
{% set partitions_to_replace = partitions_to_replace.append(
    "date_sub(current_date, interval " + (i + 1) | string + " day)"
) %}
{% endfor %}

{{config(
    materialized='incremental',
    unique_key=['event_date_dt', 'page_title', 'page_location', 'creative_name', 'promotion_name', 'creative_slot'],
    cluster_by= ['event_date_dt']
)
}}
with view_promotion as (
    select
        session_key,
        event_date_dt,
        page_title,
        page_location,
        creative_name,
        promotion_name,
        creative_slot
    from {{ref('fct_ga4__event_view_promotion')}}
    where promotion_name = 'Premium Article'
    {% if is_incremental() %} -- 
        {% if var('static_incremental_days', 1 ) %}
            and event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
),
select_promotion as (
    select
        session_key,
        event_date_dt,
        page_title,
        page_location,
        creative_name,
        promotion_name,
        creative_slot
    from {{ref('fct_ga4__event_select_promotion')}}
    where promotion_name = 'Premium Article'
    {% if is_incremental() %} -- 
        {% if var('static_incremental_days', 1 ) %}
            and event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
),
purchase as (
    select
        session_key,
        event_date_dt,
    from {{ref('fct_ga4__event_purchase')}}
    {% if is_incremental() %} -- 
        {% if var('static_incremental_days', 1 ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
)
select
    view_promotion.event_date_dt,
    view_promotion.page_title,
    view_promotion.page_location,
    view_promotion.creative_name,
    view_promotion.promotion_name,
    view_promotion.creative_slot,
    count(distinct view_promotion.session_key) as article_promotion_views,
    count(distinct select_promotion.session_key ) as article_promotion_clicks,
    count(distinct purchase.session_key) as article_promotion_purchases
from view_promotion
left join select_promotion using (session_key)
left join purchase using (session_key)
group by 1,2,3,4,5,6