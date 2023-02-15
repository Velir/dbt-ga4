-- Determines whether >=90% scroll was reached per date, page_location and session_key
select
    event_date_dt
    , page_location
    , session_key
    , countif(percent_scrolled >= 90) >= 1 as scroll_90_pct --true or false
 from 
    {{ref('stg_ga4__event_scroll')}}
group by 1,2,3