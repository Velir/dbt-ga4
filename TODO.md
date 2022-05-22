
# Misc

- DBT guide to package creation: https://docs.getdbt.com/docs/guides/building-packages
- DBT project structure notes: https://discourse.getdbt.com/t/how-we-structure-our-dbt-projects/355

# TODO

- add variable to filter out events with `debug_mode = true` - Don't see any in BQ, are they exported? 
- bring user properties into `dim_ga4__users` (variable containing a list of user properties?)
- mechanism to take in an array variable listing custom events and output 1 model per event (is this possible?)
- How to handle user_id vs. client_id?
- move common event params to `base_ga4__events`
    - engagement_time_msec
    - ga_session_id
    - session_engaged
    - page_title
    - engaged_session_event
    - percent_scrolled
    - page_referrer
    - ga_session_number
    - page_location
    - ignore_referrer
- Add event timing (avg time to next page) metrics
- Session + conversion metrics
- Handle `privacy_info` field - without analytics storage, the client ID will be null. Should remove these users from dim_users
- Create staging tables for the following events:
    - view_promotion    
    - add_to_cart
    - Audience trigger events. See https://support.google.com/analytics/answer/9934109?hl=en
    - Special treatment for conversion events?
    - Full event reference: 
        - https://developers.google.com/analytics/devguides/collection/ga4/reference/events
        - https://support.google.com/analytics/answer/9216061?hl=en&ref_topic=9756175
- Review these issues for ideas for our repo: https://github.com/coding-is-for-losers/ga4-bigquery-starter/issues
- Test whether session keys are unique
- Add integration tests - Need to think about how to handle nested data as the source. CSV won't reproduce nested data. 
    - Look into using https://github.com/EqualExperts/dbt-unit-testing and generating mock data using SQL statements. 
- intraday support
- Any special considerations for handling >1 data stream? 
- Set dynamic vs. static partitioning using a variable
- Seed file for channel group mapping
- Implement dev profile considerations to limit processing: https://docs.getdbt.com/docs/guides/best-practices#limit-the-data-processed-when-in-development
- Example of a funnel model
- Review LookML examples for inspiration: https://github.com/llooker/ga_four_block_dev/tree/master/views/event_data_dimensions
- Configuration flag to turn off ecommerce tables
- Configuration and dynamic templates to create custom event tables and dimensions
- Configuration to create custom dimensions (session, user, event_*) from event parameters

## Discussion: Set dynamic vs. static partitioning using a variable
Damon:
GA4 SLA: https://support.google.com/analytics/answer/11198161?hl=en

When I've done dynamic partitioning, I usually use two days worth of data: yesterday and the day before. This gives time to for systems to recover from errors without the data engineer needing to do anything to fix the data. However, given the longest processing time listed in the document above is Daily, 24+ hours late, the freshest, processed data on Premium XLarge properties can be two days old. 

Currently, base_ga4__events_dynamic_partition.example, picks up where the previous operation left off.

It may be better to get the last x days and reprocess and replace some data in order to allow for bugs and reprocessing on Google's side.

I don't think this is worth discussing now, but we should keep it in mind as a possible alternate solution that reduces maintenance work.

## Discussion: Configuration to create custom dimensions

Product-scope (or item-scope in GA4) custom dimensions are a much missed feature.

We can implement them, with some difficulty, mapping event properties to the custom dimension. However, it is possible that Google, presuming they add item-scoped CDs, will just add the dimension to the items array which could result in stg_ga4__items.sql automatically picking up item-scoped CDs the way that it is currently written.