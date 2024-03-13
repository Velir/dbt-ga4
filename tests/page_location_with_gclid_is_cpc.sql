-- Google has changed the combination of parameters that are used to identify a CPC source in the past.
-- In order to detect new changes, this test checks that a page_location with a gclid is classified as cpc.

{{config(
    severity = 'warn'
)}}
select
    count(event_source) as sources
    , count(event_medium) as mediums
from {{ref('stg_ga4__events')}}
where original_page_location like '%gclid%'
    and event_source != 'google'
    and event_medium != 'cpc'
having sources > 0
    or mediums > 0