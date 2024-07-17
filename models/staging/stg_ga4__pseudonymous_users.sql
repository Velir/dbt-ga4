{{
    config(
        materialized='view',
        enabled=false
        
    )
}}
select
    *
    , to_base64(md5(concat(user_pseudo_id, stream_id))) as client_key
from {{ref('base_ga4__pseudonymous_users')}}
