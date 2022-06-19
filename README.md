# GA4 DBT Package

This package connects to an exported GA4 dataset and provides useful transformations as well as report-ready dimensional models that can be used to build reports or blend GA4 data with exported GA3 data.

Features include:
- Flattened models to access common events and event parameters such as `page_view`, `session_start`, and `purchase`
- Conversion of sharded event tables into a single partitioned table
- Incremental loading of GA4 data into your staging tables 
- Session and User dimensional models
- Easy access to query parameters such as GCLID and UTM params
- Support for custom event parameters & custom user properties
- Implementation of default channel grouping from source/medium information

# Models

| model | description |
|-------|-------------|
| stg_ga4__events | Contains cleaned event data that is enhanced with useful event and session keys. |
| stg_ga4__event_* | 1 model per event (ex: page_view, purchase) which flattens event parameters specific to that event |
| stg_ga4__event_to_query_string_params | Mapping between each event and any query parameters & values that were contained in the event's `page_location` field |
| stg_ga4__user_properties | Finds the most recent occurance of specific event_params and assigns them to a user's client_id. Event params are specified as variables (see documentation below) |
| stg_ga4__sessions_traffic_sources | Finds the source, medium, and default channel grouping for each session |
| dim_ga4__users | Dimension table for users which contains attributes such as first and last page viewed. | 
| dim_ga4__sessions | Dimension table for sessions which contains useful attributes such as geography, device information, and campaign data |

# Installation & Configuration
## Install from DBT Package Hub
Add the following to your `packages.yml` file:

```
packages:
  - package: Velir/ga4
    version: [">=0.1.0", "<0.2.0"]
```

## Install From GitHub

Add the following to your `packages.yml` file:

```
packages:
  - git: "https://github.com/Velir/dbt-ga4.git"
    revision: 0.1.3
```

## Install From Local Directory

1. Clone this repository to a folder in the same parent directory as your DBT project
2. Update your project's `packages.yml` to include a reference to this package:

```
packages:
  - local: ../dbt-ga4
```
## Variables (Required)

This package assumes that you have an existing DBT project with a BigQuery profile and a BigQuery GCP instance available with GA4 event data loaded. Source data is located using the following variables which must be set in your `dbt_project.yml` file.

```
vars:
    ga4:
        project: "your_gcp_project"
        dataset: "your_ga4_dataset"
        start_date: "YYYYMMDD" # Earliest date to load
        include_intraday_events: true # false|true depending on whether an intraday event table exists
```

If you don't have any GA4 data of your own, you can connect to Google's public data set with the following settings:

```
vars:
    project: "bigquery-public-data"
    dataset: "ga4_obfuscated_sample_ecommerce"
    start_date: "20210120"
```

More info about the GA4 obfuscated dataset here: https://support.google.com/analytics/answer/10937659?hl=en#zippy=%2Cin-this-article

### Using Custom Event Parameters (Optional)

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
### Using Custom User Properties (Optional)

User-scoped event properties can be assigned using the following variable configuration in your `dbt_project.yml`. The `dim_ga4__users` dimension table will be updated to include the last value seen for each user.

```
user_properties:
  - event_parameter: "[your event parameter]"
    user_propertyname: "[a unique name for the user property]"
    value_type: "[one of string_value|int_value|float_value|double_value]"
```

For example: 

```
vars:
  ga4:
      user_properties:
        - event_parameter: "page_location"
          user_property_name: "most_recent_page_location"  
          value_type: "string_value"
        - event_parameter: "another_event_param"
          user_property_name: "most_recent_param"  
          value_type: "string_value"
```

# Connecting to BigQuery

This package assumes that BigQuery is the source of your GA4 data. Full instructions for connecting DBT to BigQuery are here: https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile

The easiest option is using OAuth with your Google Account. Summarized instructions are as follows:
 
1. Download and initialize gcloud SDK with your Google Account (https://cloud.google.com/sdk/docs/install)
2. Run the following command to provide default application OAuth access to BigQuery:

```
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/iam.test
```
# Integration Testing

This package uses `pytest` as a method of unit testing individual models. More details can be found in the [integration_tests/README.md](integration_tests) folder.
