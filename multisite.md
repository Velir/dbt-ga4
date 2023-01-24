# Multi-site

Multi-site is an advanced feature that is best implemented by someone with at least a basic knowledge of dbt. It lets you combine multiple GA4 exports into a single set of reporting tables.

TODO: Modify all downstream reporting tables to support stream_id, and stream_name
TODO: Add stream_id to stream_name seed file for friendly naming

## Using a multi-site installation

When creating your own data marts, it is best to ensure that stream_id and stream_name (if you've mapped the stream_id to friendly names in a seed file as detailed under installation) are added to all data marts so that users can view all data or select specific streams.

## Setting up multi-site

The setup process requires four basic steps.

1. Configure the ga4_datasets variable in your `dbt_project.yml`
2. Override the dbt-ga4 package `src_ga4.yml` file locally in your project with multi-site configurations
3. Create and configure `base_ga4__multisite_events_*` models for each site
4. Create a seed file and map `stream_id` values to user-friendly `stream_name` values

### Configure the ga4_datasets variable in your `dbt_project.yml`

In your `dbt_project.yml` file, add the following **project** variable which lets the package know to treat this as a multi-site installation and lets you configure multiple source datasets from BigQuery.

```
vars:
  ga4_datasets: ['111111111','222222222', '333333333']
```

Replace the numbers with the **GA4 project ID** which should also match the numeric portion of the source GA4 dataset name, 'analytics_111111111' for example.

The default `dataset` variable will not do anything when the `ga4_datasets` variable is defined.

### Override the dbt-ga4 package `src_ga4.yml` file locally in your project with multi-site configurations

Create a `src_ga4.yml` file in your project models folder wherever makes the most sense. If you are mirroring the package structure, the most likely place to put this is under `/models/staging/ga4/`. However, the location does not matter as long as the name matches the package YAML file that we need to override.

Copy and paste the below code into the new `src_ga4.yml` file.

```
version: 2

sources:
  - name: ga4_111111111
    database: "{{var('project')}}" 
    schema: "analytics_111111111" 
    tables:
      - name: events
        identifier: events_* # Scan across all sharded event tables. Use the 'start_date' variable to limit this scan
        description: Main events table exported by GA4. Sharded by date. 
      - name: events_intraday
        identifier: events_intraday_*
        description: Intraday events table which is optionally exported by GA4. Always contains events from the current day.
```

Repeat each base node (including children) under `sources:` and replace the numeric portion of `name` and `schema` with the matching **GA4 project ID** until all of your GA4 projects have been entered as shown below.

```
version: 2

sources:
  - name: ga4_111111111
    database: "{{var('project')}}" 
    schema: "analytics_111111111" 
    tables:
      - name: events
        identifier: events_* # Scan across all sharded event tables. Use the 'start_date' variable to limit this scan
        description: Main events table exported by GA4. Sharded by date. 
      - name: events_intraday
        identifier: events_intraday_*
        description: Intraday events table which is optionally exported by GA4. Always contains events from the current day.
  - name: ga4_222222222
    database: "{{var('project')}}" 
    schema: "analytics_222222222"
    tables:
      - name: events
        identifier: events_* # Scan across all sharded event tables. Use the 'start_date' variable to limit this scan
        description: Main events table exported by GA4. Sharded by date. 
      - name: events_intraday
        identifier: events_intraday_*
        description: Intraday events table which is optionally exported by GA4. Always contains events from the current day.
  - name: ga4_333333333
    database: "{{var('project')}}" 
    schema: "analytics_333333333"
    tables:
      - name: events
        identifier: events_* # Scan across all sharded event tables. Use the 'start_date' variable to limit this scan
        description: Main events table exported by GA4. Sharded by date. 
      - name: events_intraday
        identifier: events_intraday_*
        description: Intraday events table which is optionally exported by GA4. Always contains events from the current day.
```

### Create and configure `base_ga4__multisite_events_*` models for each site

For each GA4 project, you will need to copy the `tpl_base_ga4__multisite_events_.sql` file into your **dbt project** and name it `base_ga4__multisite_events_111111111.sql` substituting the numeric portion of the file with the **GA4 project ID**.

If you have the `frequency` variable set to 'daily+streaming' or you may want to set the frequency to 'daily+streaming' in the future, then you will also need to copy the `tpl_base_ga4__multisite_events_intraday_.sql` file into your **dbt project** and name it `base_ga4__multisite_events_intraday_111111111.sql` substituting the numeric portion of the file with the **GA4 project ID** in order for that setting to function.

The first two lines of each SQL file will need to be modified. Replace the numeric portion of the ds variable with your **GA4 project ID** (which should also be in the file name) in the value for the ds variable in line 1 and delete line 2.

```
{% set ds = 'ga_111111111' %} -- This should match the *numeric* portion of the GA4 source dataset and needs to be configured separately for each dataset
{{ config( enabled=false ) }}
```

### Create a seed file and map `stream_id` values to user-friendly `stream_name` values

In your seeds folder, create a seed file named `ga4_friendly_stream_names.csv`. And map the values for the `stream_id` column to user-friendly names as desired.

Please note that if you have multiple streams going in to a single GA4 property, usually because you are combining app and web streams, then you will need to map multiple values of `stream_id` in the affected GA4 projects to user-friendly names. 

Also take care that the `stream_id` is a 10-digit number and is not the same value as the 9-digit **GA4 project ID** that we have used in every other configuration step.

Your `ga4_friendly_stream_names.csv` file should look something like this.

```
stream_id,stream_name
0987654321, mysite web
1234567890, mysite ios
1122334455, mysite android
0099887766, myothersite web
```
The exact values used for `stream_name` are entirely up to you as long as they match a `stream_id` from your source datasets.

The first line must match that given in the example.

You will need to run `dbt seed` before you run the `dbt build` or `dbt run` commands.