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
    qualify row_number() over(
        partition by transaction_id
            order by event_timestamp
    ) = 1
)
select
    *
from purch
