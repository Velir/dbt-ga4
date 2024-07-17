{{
    config(
        materialized='view',
        enabled=false
    )
}}
select
    *
from {{ref('base_ga4__users')}}
