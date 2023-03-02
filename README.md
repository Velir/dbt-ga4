# GA4 DBT Package 

This [dbt](https://www.getdbt.com/) package connects to an exported GA4 dataset and provides useful transformations as well as report-ready dimensional models that can be used to build reports.

Features include:
- Flattened models to access common events and event parameters such as `page_view`, `session_start`, and `purchase`
- Conversion of sharded event tables into a single partitioned table
- Incremental loading of GA4 data into your staging tables 
- Page, session and user dimensional models with conversion counts
- Simple methods for accessing query parameters (like UTM params) or filtering query parameters (like click IDs)
- Support for custom event parameters & user properties
- Mapping from source/medium to default channel grouping

# Models

| model | description |
|-------|-------------|
| stg_ga4__events | Contains cleaned event data that is enhanced with useful event and session keys. |
| stg_ga4__event_* | 1 model per event (ex: page_view, purchase) which flattens event parameters specific to that event |
| stg_ga4__event_items | Contains item data associated with e-commerce events (Purchase, add to cart, etc) |
| stg_ga4__event_to_query_string_params | Mapping between each event and any query parameters & values that were contained in the event's `page_location` field |
| stg_ga4__user_properties | Finds the most recent occurance of specified user_properties for each user |
| stg_ga4__derived_user_properties | Finds the most recent occurance of specific event_params value and assigns them to a user_pseudo_id. Derived user properties are specified as variables (see documentation below) |
| stg_ga4__derived_session_properties | Finds the most recent occurance of specific event_params or user_properties value and assigns them to a session's session_key. Derived session properties are specified as variables (see documentation below) |
| stg_ga4__session_conversions_daily | Produces daily counts of conversions per session. The list of conversion events to include is configurable (see documentation below) |
| stg_ga4__sessions_traffic_sources | Finds the first source, medium, campaign, content, paid search term (from UTM tracking), and default channel grouping for each session. |
| dim_ga4__user_pseudo_ids | Dimension table for user devices as indicated by user_pseudo_ids. Contains attributes such as first and last page viewed.| 
| dim_ga4__sessions | Dimension table for sessions which contains useful attributes such as geography, device information, and acquisition data |
| fct_ga4__pages | Fact table for pages which aggregates common page metrics by page_location, date, and hour. |
| fct_ga4__sessions_daily | Fact table for session metrics, partitioned by date. A single session may span multiple rows given that sessions can span multiple days.  |
| fct_ga4__sessions | Fact table that aggregates session metrics across days. This table is not partitioned, so be mindful of performance/cost when querying. |

# Seeds

| seed file | description |
|-----------|-------------|
| ga4_source_categories.csv| Google's mapping between `source` and `source_category`. Downloaded from https://support.google.com/analytics/answer/9756891?hl=en |

Be sure to run `dbt seed` before you run `dbt run`.

# Installation & Configuration
## Install from DBT Package Hub
To pull the latest stable release along with minor updates, add the following to your `packages.yml` file:

```
packages:
  - package: Velir/ga4
    version: [">=2.0.0", "<2.2.0"]
```

## Install From main branch on GitHub

To install the latest code (may be unstable), add the following to your `packages.yml` file:

```
packages:
  - git: "https://github.com/Velir/dbt-ga4.git"
```

## Install From Local Directory

1. Clone this repository to a folder in the same parent directory as your DBT project
2. Update your project's `packages.yml` to include a reference to this package:

```
packages:
  - local: ../dbt-ga4
```
## Required Variables

This package assumes that you have an existing DBT project with a BigQuery profile and a BigQuery GCP instance available with GA4 event data loaded. Source data is defined using the following variables which must be set in `dbt_project.yml`.

```
vars:
  ga4:
    project: "your_gcp_project"
    dataset: "your_ga4_dataset"
    start_date: "YYYYMMDD" # Earliest date to load
    frequency: "daily" # daily|streaming|daily+streaming. See 'Export Frequency' below.
```

## Optional Variables

### Query Parameter Exclusions

Setting `query_parameter_exclusions` will remove query string parameters from the `page_location` and `page_referrer` fields for all downstream processing. Original parameters are captured in the `original_page_location` and `original_page_referrer` fields. Ex:

```
vars:
  ga4: 
    query_parameter_exclusions: ["gclid","fbclid","_ga"] 
```
### Custom Parameters

Within GA4, you can add custom parameters to any event. These custom parameters will be picked up by this package if they are defined as variables within your `dbt_project.yml` file using the following syntax:

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

You can optionally rename the output column:

```
vars:
  ga4:
    page_view_custom_parameters:
      - name: "country_code"
        value_type: "int_value"
        rename_to: "country"
```

If there are custom parameters you need on all events, you can define defaults using `default_custom_parameters`, for example:

```
vars:
  ga4:
    default_custom_parameters:
      - name: "country_code"
        value_type: "int_value"
```

### User Properties

User properties are provided by GA4 in the `user_properties` repeated field. The most recent user property for each user will be extracted and included in the `dim_ga4__users` model by configuring the `user_properties` variable in your project as follows:

```
vars:
  ga4:
    user_properties:
      - user_property_name: "membership_level"
        value_type: "int_value"
      - user_property_name: "account_status"
        value_type: "string_value"
```

### Derived User Properties

Derived user properties are different from "User Properties" in that they are derived from event parameters. This provides additional flexibility in allowing users to turn any event parameter into a user property. 

Derived User Properties are included in the `dim_ga4__users` model and contain the latest event parameter value per user. 

```
derived_user_properties:
  - event_parameter: "[your event parameter]"
    user_property_name: "[a unique name for the derived user property]"
    value_type: "[string_value|int_value|float_value|double_value]"
```

For example: 

```
vars:
  ga4:
    derived_user_properties:
      - event_parameter: "page_location"
        user_property_name: "most_recent_page_location"
        value_type: "string_value"
      - event_parameter: "another_event_param"
        user_property_name: "most_recent_param"
        value_type: "string_value"
```

### Derived Session Properties

Derived session properties are similar to derived user properties, but on a per-session basis, for properties that change slowly over time. This provides additional flexibility in allowing users to turn any event parameter into a session property. 

Derived Session Properties are included in the `fct_ga4__sessions` model and contain the latest event parameter or user property value per session.

```
derived_session_properties:
  - event_parameter: "[your event parameter]"
    session_property_name: "[a unique name for the derived session property]"
    value_type: "[string_value|int_value|float_value|double_value]"
  - user_property: "[your user property key]"
    session_property_name: "[a unique name for the derived session property]"
    value_type: "[string_value|int_value|float_value|double_value]"
```

For example: 

```
vars:
  ga4:
    derived_session_properties:
      - event_parameter: "page_location"
        session_property_name: "most_recent_page_location"
        value_type: "string_value"
      - event_parameter: "another_event_param"
        session_property_name: "most_recent_param"
        value_type: "string_value"
      - user_property: "first_open_time"
        session_property_name: "first_open_time"
        value_type: "int_value"
```

### GA4 Recommended Events

See the README file at /dbt_packages/models/staging/ga4/recommended_events for instructions on enabling [Google's recommended events](https://support.google.com/analytics/answer/9267735?hl=en).

### Conversion Events

Specific event names can be specified as conversions by setting the `conversion_events` variable in your `dbt_project.yml` file. These events will be counted against each session and included in the `fct_sessions.sql` dimensional model. Ex:

```
vars:
  ga4:
    conversion_events:['purchase','download']
```

# Incremental Loading of Event Data (and how to handle late-arriving hits)

By default, GA4 exports data into sharded event tables that use the event date as the table suffix in the format of `events_YYYYMMDD` or `events_intraday_YYYYMMDD`. This package incrementally loads data from these tables into `base_ga4__events` which is partitioned on date. There are two incremental loading strategies available:

- Dynamic incremental partitions (Default) - This strategy queries the destination table to find the latest date available. Data beyond that date range is loaded in incrementally on each run.
- Static incremental partitions - This strategy is enabled when the `static_incremental_days` variable is set to an integer. It incrementally loads in the last X days worth of data regardless of what data is availabe. Google will update the daily event tables within the last 72 hours to handle late-arriving hits so you should use this strategy if late-arriving hits is a concern. The 'dynamic incremental' strategy will not re-process past date tables. Ex: A `static_incremental_days` setting of `3` would load data from `current_date - 1` `current_date - 2` and `current_date - 3`. Note that `current_date` uses UTC as the timezone.

# Export Frequency

The value of the `frequency` variable should match the "Frequency" setting on GA4's BigQuery Linking Admin page.

| GA4 | dbt_project.yml |
|-----|-----------------|
| Daily | "daily" |
| Streaming | "streaming" |
| both Daily and Streaming | "daily+streaming" |

The daily option (default) is for sites that use just the daily, batch export. It can also be used as a substitute for the "daily+streaming" option where you don't care about including today's data so it doesn't strictly need to match the GA4 "Frequency" setting.
The streaming option is for sites that only use the streaming export. The streaming export is not constrained by Google's one million event daily limit and so is the best option for sites that may exceed that limit. Selecting both "Daily" and "Streaming" in GA4 causes the streaming, intraday BigQuery tables to be deleted when the daily, batch tables are updated.
The "daily+streaming" option uses the daily batch export and unions the streaming intraday tables. It is intended to append today's data from the streaming intraday to the batch tables.

Example:

```
vars:
  ga4:
    frequency:"daily+streaming"
```

# Connecting to BigQuery

This package assumes that BigQuery is the source of your GA4 data. Full instructions for connecting DBT to BigQuery are here: https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile

The easiest option is using OAuth with your Google Account. Summarized instructions are as follows:
 
1. Download and initialize gcloud SDK with your Google Account (https://cloud.google.com/sdk/docs/install)
2. Run the following command to provide default application OAuth access to BigQuery:

```
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/iam.test
```
# Unit Testing

This package uses `pytest` as a method of unit testing individual models. More details can be found in the [unit_tests/README.md](unit_tests) folder.

# Overriding Default Channel Groupings

By default, this package maps traffic sources to channel groupings using the `macros/default_channel_grouping.sql` macro. This macro closely adheres to Googls recommended channel groupings documented here: https://support.google.com/analytics/answer/9756891?hl=en .

Package users can override this macro and implement their own channel groupings by following these steps:
- Create a macro in your project named `default__default_channel_grouping` that accepts the same 3 arguments: source, medium, source_category
- Implement your custom logic within that macro. It may be easiest to first copy the code from the package macro and modify from there.

Overriding the package's default channel mapping makes use of dbt's dispatch override capability documented here: https://docs.getdbt.com/reference/dbt-jinja-functions/dispatch#overriding-package-macros