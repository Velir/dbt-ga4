select
    page_engagement_key,
    sum(engagement_time_msec) as page_engagement_time
from {{ ref('stg_ga4__events') }}
group by 1