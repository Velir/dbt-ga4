{#
    Clone Backfill Operations
    =========================
    Generates BigQuery scripting to clone GA4 event tables in batches.
    Use this to backfill historical data for a new property in a multi-property setup.

    Usage:
        1. Set variables via --vars flag:
           dbt compile --select clone_backfill --vars '{
               clone_property_id: 123456789,
               clone_start_date: 20240101,
               clone_end_date: 20241231
           }'

        2. Copy compiled SQL from target/compiled/ga4/analyses/clone_backfill.sql

        3. Paste and run in BigQuery console

    Variables:
        - clone_property_id (required): GA4 property ID to backfill
        - clone_start_date (required): Start date as YYYYMMDD integer
        - clone_end_date (optional): End date as YYYYMMDD integer (default: today)
        - clone_batch_size (optional): Operations per batch before pausing (default: 50)
        - clone_delay_seconds (optional): Seconds to pause between batches (default: 15)

    Notes:
        - This generates BigQuery scripting that handles batching automatically
        - The script will pause between batches to avoid hitting BigQuery rate limits
        - Progress messages are displayed via SELECT statements in BigQuery
        - Intraday tables are cloned first, then daily tables
        - When a daily table exists, the corresponding intraday clone is dropped
#}

{%- set property_id = var('clone_property_id') -%}
{%- set earliest_shard = var('clone_start_date')|int -%}
{%- set latest_shard = var('clone_end_date', modules.datetime.date.today()|string|replace("-", "")|int)|int -%}
{%- set batch_size = var('clone_batch_size', 50) -%}
{%- set delay_seconds = var('clone_delay_seconds', 15) -%}

{%- set tables = ga4.get_source_tables_to_clone(property_id, earliest_shard, latest_shard, var('source_project')) -%}
{%- set source_schema = "analytics_" ~ property_id|string -%}

-- ============================================================
-- GA4 Clone Backfill Operations
-- Property: {{ property_id }}
-- Date Range: {{ earliest_shard }} to {{ latest_shard }}
-- Total Tables: {{ tables | length }}
-- Batch Size: {{ batch_size }} | Delay: {{ delay_seconds }}s
-- ============================================================
-- Generated: {{ modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') }}
-- ============================================================

DECLARE operation_count INT64 DEFAULT 0;
DECLARE batch_size INT64 DEFAULT {{ batch_size }};
DECLARE _sleep_until TIMESTAMP;

-- Create destination schema if not exists
{{ ga4.generate_create_schema_statement(target.project, var('combined_dataset')) }}

{% if tables | length == 0 %}
-- WARNING: No tables found for property {{ property_id }} in date range {{ earliest_shard }} to {{ latest_shard }}
-- Please verify:
--   1. The property_id is correct
--   2. The source_project variable is set correctly
--   3. Tables exist in the specified date range
SELECT 'No tables found to clone. Please check your configuration.' AS warning;
{% else %}
SELECT FORMAT('Starting clone backfill: %d tables for property %s', {{ tables | length }}, '{{ property_id }}') AS status;

{% for table in tables %}
{%- set dest_table_suffix = table.date_shard ~ property_id -%}
-- [{{ loop.index }}/{{ tables | length }}] {{ table.type }}: {{ table.date_shard }}
{% if table.type == 'intraday' %}
{{ ga4.generate_clone_statement(
    var('source_project'), source_schema, table.source_table,
    target.project, var('combined_dataset'), 'events_intraday_' ~ dest_table_suffix
) }}
{% elif table.type == 'daily' %}
{{ ga4.generate_clone_statement(
    var('source_project'), source_schema, table.source_table,
    target.project, var('combined_dataset'), 'events_' ~ dest_table_suffix
) }}
{{ ga4.generate_drop_statement(
    target.project, var('combined_dataset'), 'events_intraday_' ~ dest_table_suffix
) }}
{% endif %}

SET operation_count = operation_count + 1;
IF MOD(operation_count, batch_size) = 0 THEN
    SELECT FORMAT('Completed %d of %d operations. Pausing %d seconds...', operation_count, {{ tables | length }}, {{ delay_seconds }}) AS status;
    SET _sleep_until = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {{ delay_seconds }} SECOND);
    WHILE CURRENT_TIMESTAMP() < _sleep_until DO
    END WHILE;
END IF;

{% endfor %}

SELECT FORMAT('Clone backfill complete. Processed %d operations for property %s.', operation_count, '{{ property_id }}') AS status;
{% endif %}
