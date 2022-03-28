# GA4 DBT Package

This package connects to an exported GA4 dataset and provides useful transformations as well as report-ready models that can be used alongside exported GA3 data. 

Features include:
- Flattened models to access comment events and event parameters such as `page_view`, `scroll`, and `purchase`
- Conversion of sharded event tables into a single partitioned table
- Incremental loading of GA4 data into your staging tables
- A `ga4 to ga3` data mart that implements common GA3 pre-built reports produced by Fivetran (https://fivetran.com/docs/applications/google-analytics/prebuilt-reports) 
- Session and User dimensional models
- Defaults to the public `ga4_obfuscated_sample_ecommerce` dataset provided by Google if you want to use the package and don't have your own GA4 data

# Prerequisites

- This package assumes that you have an existing DBT project with a BigQuery profile and a BigQuery GCP instance available with GA4 event data loaded. If you don't have any GA4 data of your own, you can connect to Google's public data set with the following settings:

```
vars:
    start_date: "20210120"
    project: "bigquery-public-data"
    dataset: "ga4_obfuscated_sample_ecommerce"
```

# Installation Instructions 

## Local Installation

- Clone this repository to a folder in the same parent directory as your project
- Update your packages.yml to include a reference to the local files:

```
packages:
  - local: ../dbt-ga4
```

- Add the following variables to your dbt_project.yml file denoting the source project, schema, and a start date to use when scanning GA4 event tables.

```
vars:
    ga4:
      start_date: "20210101" 
      project: "my-ga4-gcp-project"
      dataset: "analytics_00000000"
```
- Download and initialize gcloud SDK with your Google Account. Then run the following command to provide default application OAuth access to BigQuery:

```
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/iam.test
```

Full instructions for connecting to BigQuery are here: https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile#local-oauth-gcloud-setup
