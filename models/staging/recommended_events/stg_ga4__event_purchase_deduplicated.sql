{% if not flags.FULL_REFRESH %}
    {% set partitions_to_query = ['current_date'] %}
    {% for i in range(var('static_incremental_days', 1)) %}
        {% set partitions_to_query = partitions_to_query.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
{% endif %}

{{
  config(
      enabled = false,
  )
}}
with purch as (
    select
        *
    from {{ref('stg_ga4__event_purchase')}}
    {% if not flags.FULL_REFRESH %}
        where event_date_dt in ({{ partitions_to_query | join(',') }})
    {% endif %}
)
, dedup as (
    /*  this is intended to be the maximally performant MVP for transaction deduplication
        it is possible that you may want to roll up various purchase parameters and not just event_key where later events contain late-arriving parameters
        in cases like this, use this model as a template and make your customizations in your project */ 
    select distinct
        first_value(event_key ignore nulls) over (transaction_window) as event_key   
    from purch
    window transaction_window as (
        partition by transaction_id 
        order by 
            event_timestamp asc rows between unbounded preceding
            and unbounded following
    )
)
select
    purch.*
from dedup
left join purch on dedup.event_key = purch.event_key