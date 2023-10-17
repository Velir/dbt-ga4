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
select
    first_value(* ignore nulls) over (transaction_window)
from purch
window transaction_window as (
    partition by transaction_id 
    order by 
        event_timestamp asc rows between {{var('static_incremental_days', 3 ) * 24 * 60 * 60 * 1000000 }} preceding
        and unbounded following
        )