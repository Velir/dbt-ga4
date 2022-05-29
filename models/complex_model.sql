-- models/complex_model.sql
select count(*) as num from {{ source('population', 'persons') }}
union all
select count(*) as num from {{ ref('stg_persons') }}