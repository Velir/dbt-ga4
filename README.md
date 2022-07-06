# GA4 DBT Package

This package connects to an exported GA4 dataset and provides useful transformations as well as report-ready dimensional models that can be used to build reports or blend GA4 data with exported GA3 data.

Features include:
- Flattened models to access common events and event parameters such as `page_view`, `session_start`, and `purchase`
- Conversion of sharded event tables into a single partitioned table
- Incremental loading of GA4 data into your staging tables 
- Session and user dimensional models with conversion counts
- Easy access to query parameters such as GCLID and UTM params
- Support for custom event parameters & custom user properties
- Mapping from source/medium to default channel grouping
- Ability to exclude query parameters (like `fbclid`) from page paths

# Models

| model | description |
|-------|-------------|
| stg_ga4__events | Contains cleaned event data that is enhanced with useful event and session keys. |
| stg_ga4__event_* | 1 model per event (ex: page_view, purchase) which flattens event parameters specific to that event |
| stg_ga4__event_to_query_string_params | Mapping between each event and any query parameters & values that were contained in the event's `page_location` field |
| stg_ga4__user_properties | Finds the most recent occurance of specific event_params and assigns them to a user's client_id. User properties are specified as variables (see documentation below) |
| stg_ga4__session_conversions | Produces session-grouped event counts for a configurable list of event names (see documentation below) |
| stg_ga4__sessions_traffic_sources | Finds the source, medium, and default channel grouping for each session |
| dim_ga4__users | Dimension table for users which contains attributes such as first and last page viewed. | 
| dim_ga4__sessions | Dimension table for sessions which contains useful attributes such as geography, device information, and campaign data |

# Seeds

| seed file | description |
|-----------|-------------|
| ga4_source_categories.csv| Google's mapping between `source` and `source_category`. Downloaded from https://support.google.com/analytics/answer/9756891?hl=en |

Be sure to run `dbt seed` before you run `dbt run`.

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
    revision: 0.1.4
```

## Install From Local Directory

1. Clone this repository to a folder in the same parent directory as your DBT project
2. Update your project's `packages.yml` to include a reference to this package:

```
packages:
  - local: ../dbt-ga4
```
## Required Variables

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

## Optional Variables

### Query Parameter Exclusions

Setting `query_parameter_exclusions` will remove query string parameters from the `page_location` field for all downstream processing. Original parameters are captured in a new `original_page_location` field. Ex:

```
vars:
  ga4: 
    query_parameter_exclusions: ["gclid","fbclid","_ga"] 
```
### Custom Events

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
### Custom User Properties

User-scoped event properties can be assigned using the following variable configuration in your `dbt_project.yml`. The `dim_ga4__users` dimension table will be updated to include the last value seen for each user.

```
user_properties:
  - event_parameter: "[your event parameter]"
    user_property_name: "[a unique name for the user property]"
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
### Conversion Events

Specific event names can be specified as conversions by setting the `conversion_events` variable in your `dbt_project.yml` file. These events will be counted against each session and included in the `fct_sessions.sql` dimensional model. Ex:

```
vars:
  ga4:
      conversion_events:['purchase','download']
```

### Custom Item Properties

Custom item properties can be enabled against all ecommerce events by setting the `custom_item_parameters` variable in your `dbt_project.yml` file. These properties will be included in the ecommerce event's staging model where that event expects just a single item (like `select_item`) and in a stand-alone `stg_ga4__items.sql` table for events with one-to-many relationships to their items. Ex:

```
vars:
  ga4:
      custom_item_parameters:['size','color']
```
The columns will be prefixed with "item_" so the example "size" and "color" parameters would find the "items.size" and "items.color" fields in the source data and be saved as "item_size" and "item_color" in the relevant staging tables.

# Incremental Loading of Event Data

By default, GA4 exports data into sharded event tables that use the event date as the table suffix in the format of `events_YYYYMMDD`. This package incrementally loads data from these tables (and not the `intraday` tables) into `base_ga4__events` which is partitioned on date. There are two incremental loading strategies available:

- Dynamic incremental partitions (Default) - This strategy queries the destination table to find the latest date available. Data beyond that date range is loaded in incrementally on each run.

- Static incremental partitions - This strategy incrementally loads in the last X days worth of data regardless of what data is availabe. In certain cases, this improves performance because the `max(event_date)` does not need to be calculated dynamically. This is enabled when the `static_incremental_days` variable is set to an integers. A setting of `3` would load data from `current_date - 1` `current_date - 2` and `current_date - 3`. Note that `current_date` uses UTC as the timezone.

## Intraday Events

Users can enable the inclusion of intraday data by setting `include_intraday_events: true`. Intraday events are not included in incremental loads, but are instead unioned with historical events.

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
