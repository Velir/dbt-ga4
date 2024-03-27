
# TODO

- It may be overly expensive to scan ALL events looking for first/last occurances of user's event parameters. We can move data from 1st & last session into a new table and scan that table instead. 
- mechanism to take in an array variable listing custom events and output 1 model per event (is this possible?)
- Add event timing (avg time to next page) metrics
- Anything else to do with `privacy_info` field? Right now removing 'null' client ids from user dim tables. 
- Create staging tables for the following events:
    - view_promotion    
    - add_to_cart
    - Audience trigger events. See https://support.google.com/analytics/answer/9934109?hl=en
    - Special treatment for conversion events?
    - Full event reference: 
        - https://developers.google.com/analytics/devguides/collection/ga4/reference/events
        - https://support.google.com/analytics/answer/9216061?hl=en&ref_topic=9756175
- Review these issues for ideas for our repo: https://github.com/coding-is-for-losers/ga4-bigquery-starter/issues
- Any special considerations for handling >1 data stream? 
- Implement dev profile considerations to limit processing: https://docs.getdbt.com/docs/guides/best-practices#limit-the-data-processed-when-in-development
- Example of a funnel model https://github.com/teej/sf-funnels
- Review LookML examples for inspiration: https://github.com/llooker/ga_four_block_dev/tree/master/views/event_data_dimensions
    - Add landing page / exit page, session start/end time, session duration, is bounce, campaign source to `dim_sessions` model
- Configuration and dynamic templates to create custom event tables and dimensions
- Configuration to create custom dimensions (session, user, event_*) from event parameters
- Use Fivetran's `union_data` method (or something similar) to handle multiple, unioned GA4 exports. https://github.com/fivetran/dbt_xero_source/blob/main/models/tmp/stg_xero__account_tmp.sql

## Misc

- DBT guide to package creation: https://docs.getdbt.com/docs/guides/building-packages
- DBT project structure notes: https://discourse.getdbt.com/t/how-we-structure-our-dbt-projects/355

## Discussion: Configuration to create custom dimensions

Product-scope (or item-scope in GA4) custom dimensions are a much missed feature.

We can implement them, with some difficulty, mapping event properties to the custom dimension. However, it is possible that Google, presuming they add item-scoped CDs, will just add the dimension to the items array which could result in stg_ga4__items.sql automatically picking up item-scoped CDs the way that it is currently written.
