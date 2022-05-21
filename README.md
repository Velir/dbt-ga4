# GA4 DBT Package

This package connects to an exported GA4 dataset and provides useful transformations as well as report-ready dimensional models that can be used to build reports or blend GA4 data with exported GA3 data.

Features include:
- Flattened models to access common events and event parameters such as `page_view`, `session_start`, and `purchase`
- Conversion of sharded event tables into a single partitioned table
- Incremental loading of GA4 data into your staging tables 
- Session and User dimensional models
- Support for custom event parameters

# Prerequisites

- This package assumes that you have an existing DBT project with a BigQuery profile and a BigQuery GCP instance available with GA4 event data loaded. If you don't have any GA4 data of your own, you can connect to Google's public data set with the following settings:

```
vars:
    start_date: "20210120"
    project: "bigquery-public-data"
    dataset: "ga4_obfuscated_sample_ecommerce"
```

More info here: https://support.google.com/analytics/answer/10937659?hl=en#zippy=%2Cin-this-article

# Installation Instructions 

## Local Installation

1. Clone this repository to a folder in the same parent directory as your DBT project
2. Update your project's `packages.yml` to include a reference to this package:

```
packages:
  - local: ../dbt-ga4
```

3. Add the following variables to your dbt_project.yml file denoting the source GCP project, dataset, and a start date to use when scanning sharded GA4 event tables.

```
vars:
    ga4:
      start_date: "20210101" 
      project: "my-ga4-gcp-project"
      dataset: "analytics_00000000"
```

# Handling Custom Parameters

One important feature of GA4 is that you can add custom parameters to any event. These custom parameters will be picked up by this package if they are defined as variables within your `dbt_project.yml` file using the following syntax:

```
[event name]_custom_parameters
  - name: "[name of custom parameter]"
    value_type: "[string_value|int_value|float_value|double_value]"
```

For example: 

```
vars:
  ga4:
    page_view_custom_parameters:
          - name: "clean_event"
            value_type: "string_value"
          - name: "country_code"
            value_type: "int_value"
```

# Connecting to BigQuery

Full instructions for connecting DBT to BigQuery are here: https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile

The easiest option is using OAuth with your Google Account. Summarized instructions are as follows:
 
1. Download and initialize gcloud SDK with your Google Account (https://cloud.google.com/sdk/docs/install)
2. Run the following command to provide default application OAuth access to BigQuery:

```
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/iam.test
```

