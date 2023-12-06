with base_pseudo_users as (
    select * from {{ ref('base_ga4__pseudonymous_users') }}
)
-- Add key that captures a combination of stream_id and user_pseudo_id to uniquely identify a 'client' (aka. a device) within a single stream
, include_client_key as (
    select *
    , to_base64(md5(concat(user_pseudo_id, stream_id))) as client_key
    from base_pseudo_users
)
, include_user_properties as (
select * from include_client_key
-- I think I want to just unnest the user properties as they appear in the source tables rather than enrich them here to keep the base models slim
-- I don't have a sample data set that is setting user properties
-- I think this might still work though, just can't test it currently with data
-- The new user tables are very different from our implementation. Reconciling them will be a pain
{% if var('derived_user_properties', false) %}
-- If derived user properties have been assigned as variables, join them on the client_key
left join {{ref('stg_ga4__derived_user_properties')}} using (client_key)
{% endif %}
{% if var('user_properties', false) %}
-- If user properties have been assigned as variables, join them on the client_key
left join {{ref('stg_ga4__user_properties')}} using (client_key)
{% endif %}

)

select * from include_user_properties