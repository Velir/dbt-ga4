-- inventory of all sessions with dimensions and metrics such as:
-- dims: channel grouping, device, geo, starttime, endtime 
-- metrics: # of events, session event value, session purchase value

with session_start_dims (
    select 
        client_id,
        user_id,
        event_date_dt as session_date,
        session_traffic_source_name,
        session_traffic_source_source,
        session_traffic_source_medium,
        ga_session_number,
        page_location as landing_page

    from {{ref("stg_ga4__event_session_start")}}

)

select * from session_start_dims