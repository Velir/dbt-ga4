# Analyses

This folder contains dbt analyses that generate SQL for manual execution. Analyses are compiled but not executed by dbt, allowing you to generate complex SQL that can be run directly in BigQuery.

## Clone Backfill

Clones GA4 event tables in batches for multi-property setups. Use this when you need to backfill historical data for a new property or perform a large initial clone operation that would otherwise timeout during a normal dbt run.

There are two ways to run a clone backfill:

1. **`dbt run-operation clone_backfill`** (recommended) - Executes directly from dbt, no copy/paste needed
2. **`clone_backfill.sql` analysis** - Generates SQL for manual execution in BigQuery console

### When to Use

- Adding a new property to an existing multi-property setup
- Initial setup of a multi-property configuration with historical data
- Re-cloning data after an issue with the combined dataset
- Any scenario where the automatic cloning in `combine_property_data` times out

### Prerequisites

- Multi-property configuration must be set up (see main README)
- `source_project` and `combined_dataset` variables must be configured in your `dbt_project.yml`

### Option 1: run-operation (Recommended)

Run the clone backfill directly from dbt. Each clone is executed as an individual query, avoiding the timeout that occurs when batching all clones together.

```bash
dbt run-operation clone_backfill --args '{
    clone_property_id: 123456789,
    clone_start_date: 20240101,
    clone_end_date: 20241231
}'
```

Progress is logged to the console as each table is cloned:

```
============================================================
GA4 Clone Backfill
============================================================
Property:    123456789
Date Range:  20240101 to 20241231
Tables:      365
============================================================

[1/365] Cloned daily: 20240101
[2/365] Cloned daily: 20240102
...
Clone backfill complete. Processed 365 tables for property 123456789.
```

#### Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `clone_property_id` | Yes | - | GA4 property ID to backfill |
| `clone_start_date` | Yes | - | Start date as YYYYMMDD integer |
| `clone_end_date` | No | Today | End date as YYYYMMDD integer |

### Option 2: Analysis (Manual SQL)

Compile the analysis to generate BigQuery scripting SQL, then run it manually in the BigQuery console.

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

#### Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `clone_property_id` | Yes | - | GA4 property ID to backfill |
| `clone_start_date` | Yes | - | Start date as YYYYMMDD integer |
| `clone_end_date` | No | Today | End date as YYYYMMDD integer |
| `clone_batch_size` | No | 50 | Number of operations per batch before pausing |
| `clone_delay_seconds` | No | 15 | Seconds to pause between batches |

### After the Backfill

After either method completes, run dbt with `clone_disabled: true` to prevent the automatic clone operation from running again:

```bash
dbt run --select base_ga4__events+ --full-refresh --vars '{clone_disabled: true}'
```

This processes the cloned data without triggering another clone operation.

### How It Works

Both methods:

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
