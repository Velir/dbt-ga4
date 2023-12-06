with base_users as (
    select * from {{ ref('base_ga4__users') }}
)
, include_user_properties as (
select * from base_users
-- I think I want to just unnest the user properties as they appear in the source tables rather than enrich them here to keep the base models slim
-- I don't have a sample data set that is setting user properties
-- I think this might still work though, just can't test it currently with data
-- The new user tables are very different from our implementation. Reconciling them will be a pain
-- Our implementation works off of the client_key which doesn't exist in the user tables
-- So we either duplicate that work or start with a more minimal implementation and see about re-adding later

)

select * from include_user_properties