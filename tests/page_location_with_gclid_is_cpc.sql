-- Google has changed the combination of parameters that are used to identify a CPC source in the past.
-- In order to detect new changes, this test checks that a page_location with a gclid is classified as cpc.
-- The purpose of this test is to detect changes in the source classification of CPC traffic.
-- As a result, it defaults to testing only the last static_incremental_days worth of tests.

{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days', 1)) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}
{{config(
    severity = 'warn'
)}}
select
    count(event_source) as sources
    , count(event_medium) as mediums
from {{ref('stg_ga4__events')}}
where page_location like '%gclid%'
    and event_source != 'google'
    and event_medium != 'cpc'
    and event_date_dt in ({{ partitions_to_replace | join(',') }})
having sources > 0
    or mediums > 0