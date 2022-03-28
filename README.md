# Installation Instructions

## Local Installation

- Update your packages.yml to include a reference to the local files:

```
packages:
  - local: ../dbt-ga4
```

- Add the following variables to your dbt_project.yml file denoting the source project, schema, and starting date range

```
vars:
    ga4:
      start_date: "20210101" base events model. 
      project: "my-ga4-gcp-project"
      dataset: "analytics_00000000"
```
- Download and initialize gcloud SDK with your Google Account. Then run the following command to provide default application OAuth access to BigQuery:

```
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/iam.test
```

Full instructions for connecting to BigQuery are here: https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile#local-oauth-gcloud-setup

------------

# Misc

DBT guide to package creation: https://docs.getdbt.com/docs/guides/building-packages
DBT project structure notes: https://discourse.getdbt.com/t/how-we-structure-our-dbt-projects/355

# TODO

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
- Review these issues for ideas for our repo: https://github.com/coding-is-for-losers/ga4-bigquery-starter/issues
- Investigate dupe event keys (ex: wn4MuaFqh3nq1t/JzgaB8w==)
    Ex: select * from `velir-website-analytics.dbt_dev_aribaudo.stg_ga4__events` where TO_BASE64(event_key)  ='wn4MuaFqh3nq1t/JzgaB8w=='
    - Example code picking first event based on window function: https://github.com/coding-is-for-losers/ga4-bigquery-starter/blob/main/models/base/dedup_events.sql

- Test whether session keys are unique
- Recreate common Fivetran ga3 models with ga4 data
    - https://fivetran.com/docs/applications/google-analytics/prebuilt-reports#traffic

- Convert basic unnesting operations into macros
- Add integration tests
- Window functions to sequence pageviews, purchases, etc. Mark the first and last

- spec out some output reports
- intraday support
- think through handling of 1 stream, multiple streams
- Set dynamic vs. static partitioning using a variable
- Seed file for channel grouping