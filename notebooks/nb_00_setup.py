# Databricks notebook source
# ============================================================
# nb_00_setup
# One-time setup notebook.
# Creates table_migration_config in Unity Catalog and inserts
# the two POC rows (samples.tpch.customer and orders).
#
# Includes all column additions that were previously separate
# ALTER TABLE statements:
#   - merge_keys        (added for MERGE INTO join condition)
#   - file_format       (added for Autoloader support)
#   - autoloader_mode   (added for Autoloader support)
#   - schema_hints      (added for Autoloader support)
#
# Run once before any other notebook in this framework.
# ============================================================

# COMMAND ----------

dbutils.widgets.text("admin_catalog",  "it")
dbutils.widgets.text("config_schema",  "migration_config")
dbutils.widgets.text("target_catalog", "it")
dbutils.widgets.text("target_schema",  "tpch_bronze")

ADMIN_CATALOG  = dbutils.widgets.get("admin_catalog")
CONFIG_SCHEMA  = dbutils.widgets.get("config_schema")
TARGET_CATALOG = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA  = dbutils.widgets.get("target_schema")

print(f"Admin catalog  : {ADMIN_CATALOG}")
print(f"Config schema  : {CONFIG_SCHEMA}")
print(f"Target catalog : {TARGET_CATALOG}")
print(f"Target schema  : {TARGET_SCHEMA}")

# COMMAND ----------

# ── Create schemas if not exists ──────────────────────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ADMIN_CATALOG}.{CONFIG_SCHEMA}")
print(f"✅ Schema {ADMIN_CATALOG}.{CONFIG_SCHEMA} ready")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"✅ Schema {TARGET_CATALOG}.{TARGET_SCHEMA} ready")

# COMMAND ----------

# ── Create table_migration_config ─────────────────────────────
# All columns included here — no ALTER TABLE scripts needed.
# Columns are grouped by function for readability.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config (

  -- Identity
  table_id              STRING    COMMENT 'Unique ID for this migration row',

  -- Source
  source_type           STRING    COMMENT 'snowflake | sqlserver | postgresql | redshift | oracle | delta | adls',
  src_database          STRING    COMMENT 'Source database name or foreign catalog instance identifier',
  src_schema            STRING    COMMENT 'Source schema name',
  src_table             STRING    COMMENT 'Source table name',

  -- Target
  target_catalog        STRING    COMMENT 'Unity Catalog target catalog',
  target_schema         STRING    COMMENT 'Unity Catalog target schema',
  target_table          STRING    COMMENT 'Delta target table name',

  -- Load pattern
  load_strategy         STRING    COMMENT 'full | incremental',
  load_mode             STRING    COMMENT 'overwrite | append | merge | copy_into | autoloader',

  -- Incremental / watermark
  incremental_col       STRING    COMMENT 'Watermark column name — nullable',
  incremental_col_type  STRING    COMMENT 'timestamp | date | integer | none',
  last_loaded_value     STRING    COMMENT 'High watermark from last successful run',
  batch_filter          STRING    COMMENT 'Optional WHERE clause for full load',

  -- Keys
  primary_keys          STRING    COMMENT 'Comma-separated primary key columns',
  merge_keys            STRING    COMMENT 'Comma-separated columns for MERGE INTO join condition — defaults to primary_keys if null',

  -- Clustering
  clustering_type       STRING    COMMENT 'liquid | partition | none',
  cluster_by            STRING    COMMENT 'Comma-separated columns for liquid clustering',
  partition_col         STRING    COMMENT 'Partition column (clustering_type=partition only)',

  -- Orchestration
  process_group         STRING    COMMENT 'Logical group — maps to a Databricks Workflow',
  process_subgroup      STRING    COMMENT 'Optional sub-group within a process group',
  depends_on_group      STRING    COMMENT 'process_group that must complete first — nullable',

  -- Credentials
  src_secret_scope      STRING    COMMENT 'Databricks secret scope for source credentials — nullable for foreign catalog reads',

  -- Control
  enabled               BOOLEAN   COMMENT 'Include in migration runs — default false',
  priority              INT       COMMENT 'Run order within process group (lower = first)',

  -- Run state
  last_run_status       STRING    COMMENT 'PENDING | RUNNING | PASSED | FAILED',
  last_run_at           TIMESTAMP COMMENT 'Timestamp of last run',
  last_sf_row_count     BIGINT    COMMENT 'Row count read from source on last run',
  last_delta_count      BIGINT    COMMENT 'Row count in Delta after last run',

  -- Autoloader
  file_format           STRING    COMMENT 'parquet | json | csv — source file format for Autoloader and ADLS loads',
  autoloader_mode       STRING    COMMENT 'availableNow | continuous — trigger mode for Autoloader',
  schema_hints          STRING    COMMENT 'Optional schema hints for Autoloader e.g. col1 INT col2 STRING',

  -- Notes
  notes                 STRING    COMMENT 'Free-text notes and error messages for engineers'
)
USING DELTA
COMMENT 'Central config and run-state table for the migration framework'
""")
print("✅ table_migration_config created")

# COMMAND ----------

# ── Insert POC rows ───────────────────────────────────────────
# Two tables from samples.tpch (Databricks built-in sample data):
#   customer — full overwrite, priority 1  (~150K rows)
#   orders   — incremental merge on o_orderdate, priority 2  (~1.5M rows)
#
# src_database = 'samples'  (foreign catalog name in Unity Catalog)
# src_schema   = 'tpch'
# Column naming convention: two-letter table prefix e.g. c_ o_

spark.sql(f"""
INSERT INTO {ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config
VALUES
(
  'tpch_customer_001',        -- table_id
  'snowflake',                -- source_type
  'samples',                  -- src_database  (foreign catalog)
  'tpch',                     -- src_schema
  'customer',                 -- src_table
  '{TARGET_CATALOG}',         -- target_catalog
  '{TARGET_SCHEMA}',          -- target_schema
  'tpch_customer',            -- target_table
  'full',                     -- load_strategy
  'overwrite',                -- load_mode
  NULL,                       -- incremental_col
  'none',                     -- incremental_col_type
  NULL,                       -- last_loaded_value
  NULL,                       -- batch_filter
  'c_custkey',                -- primary_keys
  'c_custkey',                -- merge_keys
  'none',                     -- clustering_type
  NULL,                       -- cluster_by
  NULL,                       -- partition_col
  'TPCH_POC',                 -- process_group
  'tpch',                     -- process_subgroup
  NULL,                       -- depends_on_group
  NULL,                       -- src_secret_scope (not needed — foreign catalog)
  false,                      -- enabled
  1,                          -- priority (runs first)
  'PENDING',                  -- last_run_status
  NULL,                       -- last_run_at
  NULL,                       -- last_sf_row_count
  NULL,                       -- last_delta_count
  NULL,                       -- file_format
  NULL,                       -- autoloader_mode
  NULL,                       -- schema_hints
  'POC — full overwrite. samples.tpch.customer ~150K rows. No watermark. Runs before orders.'
),
(
  'tpch_orders_001',          -- table_id
  'snowflake',                -- source_type
  'samples',                  -- src_database  (foreign catalog)
  'tpch',                     -- src_schema
  'orders',                   -- src_table
  '{TARGET_CATALOG}',         -- target_catalog
  '{TARGET_SCHEMA}',          -- target_schema
  'tpch_orders',              -- target_table
  'incremental',              -- load_strategy
  'merge',                    -- load_mode
  'o_orderdate',              -- incremental_col
  'date',                     -- incremental_col_type
  NULL,                       -- last_loaded_value (null = first run loads all)
  NULL,                       -- batch_filter
  'o_orderkey',               -- primary_keys
  'o_orderkey',               -- merge_keys
  'liquid',                   -- clustering_type
  'o_orderdate',              -- cluster_by
  NULL,                       -- partition_col
  'TPCH_POC',                 -- process_group
  'tpch',                     -- process_subgroup
  NULL,                       -- depends_on_group
  NULL,                       -- src_secret_scope (not needed — foreign catalog)
  false,                      -- enabled
  2,                          -- priority (runs after customer)
  'PENDING',                  -- last_run_status
  NULL,                       -- last_run_at
  NULL,                       -- last_sf_row_count
  NULL,                       -- last_delta_count
  NULL,                       -- file_format
  NULL,                       -- autoloader_mode
  NULL,                       -- schema_hints
  'POC — incremental merge on o_orderdate. samples.tpch.orders ~1.5M rows. Priority 2 — after customer.'
)
""")
print("✅ POC rows inserted (enabled=false — set to true when ready to run)")

# COMMAND ----------

# ── Verify ────────────────────────────────────────────────────
display(spark.sql(f"""
  SELECT
    table_id,
    src_database || '.' || src_schema || '.' || src_table AS source,
    target_catalog || '.' || target_schema || '.' || target_table AS target,
    load_strategy, load_mode,
    incremental_col, primary_keys, merge_keys,
    process_group, priority, enabled, last_run_status
  FROM {ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config
  ORDER BY priority
"""))

# COMMAND ----------

# ── Quick connection test ─────────────────────────────────────
print("Testing foreign catalog reads...")
try:
    cust_count = spark.sql("SELECT COUNT(*) AS cnt FROM samples.tpch.customer").collect()[0]['cnt']
    print(f"✅ samples.tpch.customer : {cust_count:,} rows")
except Exception as e:
    print(f"❌ samples.tpch.customer : {str(e)[:120]}")

try:
    ord_count = spark.sql("SELECT COUNT(*) AS cnt FROM samples.tpch.orders").collect()[0]['cnt']
    print(f"✅ samples.tpch.orders   : {ord_count:,} rows")
except Exception as e:
    print(f"❌ samples.tpch.orders   : {str(e)[:120]}")

print("""
Setup complete. Next steps:
  1. Review rows above — confirm source/target paths look correct
  2. Run nb_02_workflow_init + nb_03_dispatcher + nb_04_workflow_summary
     OR create a Databricks Workflow with those three tasks
  3. Set enabled = true for both rows before triggering
     UPDATE it.migration_config.table_migration_config
     SET enabled = true
     WHERE process_group = 'TPCH_POC';
""")
