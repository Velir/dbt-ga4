# Prerequisites

- This package assumes that you have a project with a BigQuery profile and a BigQuery GCP instance available with GA4 event data loaded.

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
