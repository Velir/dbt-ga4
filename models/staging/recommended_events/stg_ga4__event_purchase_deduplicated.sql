{{
  config(
      enabled = false,
  )
}}
with purch as (
    select
        *
    from {{ref('stg_ga4__event_purchase')}}
)
, dedup as (
    /*  this is intended to be the maximally performant MVP for transaction deduplication
        it is possible that you may want to roll up various purchase parameters and not just event_key where later events contain late-arriving parameters
        in cases like this, use this model as a template and make your customizations in your project */ 
    select
        first_value(event_key ignore nulls) over (transaction_window) as event_key   
    from purch
    window transaction_window as (
        partition by transaction_id 
        order by 
            event_timestamp asc rows between {{var('static_incremental_days', 3 ) * 24 * 60 * 60 * 1000000 }} preceding
            and unbounded following
    )
)
select
    purch.*
from dedup
left join purch on dedup.event_key = purch.event_key