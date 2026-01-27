# Analyses

This folder contains dbt analyses that generate SQL for manual execution. Analyses are compiled but not executed by dbt, allowing you to generate complex SQL that can be run directly in BigQuery.

## clone_backfill.sql

Generates BigQuery scripting to clone GA4 event tables in batches for multi-property setups. Use this when you need to backfill historical data for a new property or perform a large initial clone operation that would otherwise timeout during a normal dbt run.

### When to Use

- Adding a new property to an existing multi-property setup
- Initial setup of a multi-property configuration with historical data
- Re-cloning data after an issue with the combined dataset
- Any scenario where the automatic cloning in `combine_property_data` times out

### Prerequisites

- Multi-property configuration must be set up (see main README)
- `source_project` and `combined_dataset` variables must be configured in your `dbt_project.yml`

### Usage

1. **Compile the analysis with your desired parameters:**

```bash
dbt compile --select clone_backfill --vars '{
    clone_property_id: 123456789,
    clone_start_date: 20240101,
    clone_end_date: 20241231
}'
```

2. **Find the compiled SQL:**

The compiled output will be at:
```
target/compiled/ga4/analyses/clone_backfill.sql
```

3. **Run in BigQuery:**

Copy the compiled SQL and paste it into the BigQuery console. The script will:
- Create the destination schema if it doesn't exist
- Clone tables in batches with automatic pausing between batches
- Display progress messages as it runs

4. **Run dbt with cloning disabled:**

After the backfill completes, run dbt with `clone_disabled: true` to prevent the automatic clone operation from running again:

```bash
dbt run --select base_ga4__events+ --full-refresh --vars '{clone_disabled: true}'
```

This processes the cloned data without triggering another clone operation.

### Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `clone_property_id` | Yes | - | GA4 property ID to backfill |
| `clone_start_date` | Yes | - | Start date as YYYYMMDD integer |
| `clone_end_date` | No | Today | End date as YYYYMMDD integer |
| `clone_batch_size` | No | 50 | Number of operations per batch before pausing |
| `clone_delay_seconds` | No | 15 | Seconds to pause between batches |

### Example

To backfill all of 2024 for property 123456789:

```bash
dbt compile --select clone_backfill --vars '{
    clone_property_id: 123456789,
    clone_start_date: 20240101,
    clone_end_date: 20241231,
    clone_batch_size: 50,
    clone_delay_seconds: 15
}'
```

### How It Works

The generated script uses BigQuery scripting to:

1. Discover all `events_*` and `events_intraday_*` tables for the specified property within the date range
2. Clone each table to the combined dataset with the property ID appended to the table name
3. Drop any intraday clones when a corresponding daily table exists
4. Pause between batches to avoid hitting BigQuery rate limits (50 partition operations per 10 seconds)

### Customization

The underlying macros use dbt's dispatch pattern, so you can override them in your project if needed:

- `ga4.get_source_tables_to_clone()` - Discovers tables to clone
- `ga4.generate_clone_statement()` - Generates CREATE TABLE ... CLONE statements
- `ga4.generate_drop_statement()` - Generates DROP TABLE statements
- `ga4.generate_create_schema_statement()` - Generates CREATE SCHEMA statements
