DBT guide to package creation: https://docs.getdbt.com/docs/guides/building-packages
DBT project structure notes: https://discourse.getdbt.com/t/how-we-structure-our-dbt-projects/355

To connect to BigQuery using OAuth, see instructions here: https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile#local-oauth-gcloud-setup

# Configuration Instructions

Create the following variables scoped to the ga4 package in your dbt_project.yml
- project (defaults to "bigquery-public-data")
- dataset (defaults to "ga4_obfuscated_sample_ecommerce")

# TODO

- Rerun everything using a new GA4 dataset
- Create staging tables for the following events:
    - scroll
    - first_visit
    - view_promotion
    - click
    - add_to_cart
    - purchase
    - Full event reference: https://developers.google.com/analytics/devguides/collection/ga4/reference/events
- Create stg_sessions model
- Create stg_users model


- Recreate common Fivetran ga3 models with ga4 data
    - https://fivetran.com/docs/applications/google-analytics/prebuilt-reports#traffic

- Convert basic unnesting operations into macros
- Add integration tests
- Window functions to sequence pageviews, purchases, etc. Mark the first and last

- spec out some output reports
- intraday support
- bring in sample queries from Google https://support.google.com/analytics/answer/9037342?hl=en&ref_topic=9359001#zippy=%2Cin-this-article
- better handling of streams
- Set dynamic vs. static partitioning using a variable
- Seed file for channel grouping