select *,
    {{ bq_unnest('event_params', 'ga_session_number') }}
 from {{ref('stg_ga4__events')}},
    UNNEST(event_params) as event_params