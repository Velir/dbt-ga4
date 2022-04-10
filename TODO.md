
------------

# Misc

DBT guide to package creation: https://docs.getdbt.com/docs/guides/building-packages
DBT project structure notes: https://discourse.getdbt.com/t/how-we-structure-our-dbt-projects/355

# TODO

- Create staging tables for the following events:
    - scroll
    - first_visit
    - view_promotion    
    - add_to_cart
    - purchase
    - Audience entrance/exist conversion events
    - Full event reference: https://developers.google.com/analytics/devguides/collection/ga4/reference/events
- Create dim_sessions model
- Create dim_users model
- Review these issues for ideas for our repo: https://github.com/coding-is-for-losers/ga4-bigquery-starter/issues
- Test whether session keys are unique
- Recreate common Fivetran ga3 models with ga4 data
    - https://fivetran.com/docs/applications/google-analytics/prebuilt-reports#traffic

- Add integration tests
- intraday support
- think through handling of 1 stream, multiple streams
- Set dynamic vs. static partitioning using a variable
- Seed file for channel grouping
- Implement dev profile considerations to limit processing: https://docs.getdbt.com/docs/guides/best-practices#limit-the-data-processed-when-in-development