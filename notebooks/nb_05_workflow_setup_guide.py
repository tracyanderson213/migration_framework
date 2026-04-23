# Databricks notebook source
 ============================================================
 nb_05_workflow_setup_guide
 Reference notebook — not executed as part of the workflow.
 Documents how to configure the Databricks Workflow for
 each process_group.
 ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC  ### Databricks Workflow Setup Guide
# MAGIC  ### One Workflow per process_group
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  ### Workflow Structure
# MAGIC
# MAGIC  Each Workflow has **3 tasks** in sequence:
# MAGIC
# MAGIC  ```
# MAGIC  Task 1: init      → nb_02_workflow_init
# MAGIC      ↓
# MAGIC  Task 2: dispatch  → nb_03_dispatcher     (depends on: init)
# MAGIC      ↓
# MAGIC  Task 3: summary   → nb_04_workflow_summary (depends on: dispatch)
# MAGIC  ```
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  ### Creating a Workflow in Databricks UI
# MAGIC
# MAGIC  1. Go to **Workflows → Create Job**
# MAGIC  2. Name the job: `{process_group}_migration` e.g. `SNOWFLAKE_SAMPLE_DATA_migration`
# MAGIC  3. Set Global Parameters (shared across all tasks):
# MAGIC     - process_group  = TPCH_POC
# MAGIC     - admin_catalog  = it
# MAGIC     - config_schema  = migration_config
# MAGIC     - max_workers    = 8
# MAGIC     - adls_base_path = abfss://migration@storageaccount.dfs.core.windows.net
# MAGIC  4. Add **Task 1**:
# MAGIC     - Task name: `init`
# MAGIC     - Type: `Notebook`
# MAGIC     - Path: `/path/to/nb_02_workflow_init`
# MAGIC     - Cluster: your shared cluster or job cluster
# MAGIC  5. Add **Task 2**:
# MAGIC     - Task name: `dispatch`
# MAGIC     - Type: `Notebook`
# MAGIC     - Path: `/path/to/nb_03_dispatcher`
# MAGIC     - Depends on: `init`
# MAGIC  6. Add **Task 3**:
# MAGIC     - Task name: `summary`
# MAGIC     - Type: `Notebook`
# MAGIC     - Path: `/path/to/nb_04_workflow_summary`
# MAGIC     - Depends on: `dispatch`
# MAGIC  6. Set **Job Parameters** (shared across all tasks):
# MAGIC      - process_group
# MAGIC      - admin_catalog
# MAGIC      -  config_schema
# MAGIC      -  max_workers
# MAGIC      - adls_base_path
# MAGIC   
# MAGIC  ---
# MAGIC
# MAGIC  ## Before Running
# MAGIC
# MAGIC  1. Run `nb_00_setup` once to create `table_migration_config`
# MAGIC  2. Run `nb_01_bootstrap_migration_config` to populate rows from Snowflake
# MAGIC  3. Review rows in `table_migration_config` — fill in:
# MAGIC     - `primary_keys` and `merge_keys` for incremental tables
# MAGIC     - `incremental_col` and `incremental_col_type`
# MAGIC     - `cluster_by` columns if `clustering_type = 'liquid'`
# MAGIC     - `priority` for tables with dependencies
# MAGIC  4. Set `enabled = true` for tables ready to run
# MAGIC  5. Trigger the Workflow

# COMMAND ----------

# ── Show current config state ─────────────────────────────────
dbutils.widgets.text("admin_catalog", "it")
dbutils.widgets.text("config_schema", "migration_config")
dbutils.widgets.text("process_group", "TPCH_POC")

ADMIN_CATALOG = dbutils.widgets.get("admin_catalog")
CONFIG_SCHEMA = dbutils.widgets.get("config_schema")
PROCESS_GROUP = dbutils.widgets.get("process_group")

display(spark.sql(f"""
    SELECT
        table_id,
        src_database,
        src_schema,
        src_table,
        load_strategy,
        load_mode,
        incremental_col,
        primary_keys,
        merge_keys,
        clustering_type,
        cluster_by,
        process_group,
        priority,
        enabled,
        last_run_status,
        last_run_at,
        last_sf_row_count,
        last_delta_count,
        last_loaded_value
    FROM {ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config
    WHERE process_group = '{PROCESS_GROUP}'
    ORDER BY priority, table_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Re-running Failed Tables
# MAGIC
# MAGIC  To re-run only failed tables without touching passed ones:
# MAGIC
# MAGIC  ```sql
# MAGIC  -- Reset only failed rows to PENDING + re-enable
# MAGIC  UPDATE it.migration_config.table_migration_config
# MAGIC  SET last_run_status = 'PENDING',
# MAGIC      enabled         = true
# MAGIC  WHERE process_group    = 'SNOWFLAKE_SAMPLE_DATA'
# MAGIC    AND last_run_status  = 'FAILED';
# MAGIC  ```
# MAGIC
# MAGIC  Then re-trigger the Workflow.
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  ### Full Re-run (reset all)
# MAGIC
# MAGIC  ```sql
# MAGIC  UPDATE it.migration_config.table_migration_config
# MAGIC  SET last_run_status   = 'PENDING',
# MAGIC      last_loaded_value = NULL,
# MAGIC      last_sf_row_count = NULL,
# MAGIC      last_delta_count  = NULL
# MAGIC  WHERE process_group = 'TPCH_POC'
# MAGIC    AND enabled       = true;
# MAGIC  ```
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC  ### Adding a New Process Group
# MAGIC
# MAGIC  1. Run `nb_01_bootstrap_migration_config` with `process_group` = new group name
# MAGIC  2. Create a new Workflow in Databricks UI
# MAGIC  3. Set job parameter `process_group` = new group name
# MAGIC # 4. All other tasks and parameters stay the same

# COMMAND ----------

# MAGIC %md
# MAGIC  ### load_mode Reference
# MAGIC
# MAGIC  | load_mode | load_strategy | Mechanism | Use When |
# MAGIC  |---|---|---|---|
# MAGIC  | `overwrite` | full | Read all rows → Delta overwrite | Small/medium tables, reference data |
# MAGIC  | `append` | full | Read all rows → Delta append | Audit/log tables, never updated |
# MAGIC  | `merge` | incremental | Watermark filter → MERGE INTO | Fact/dim tables with updates |
# MAGIC  | `copy_into` | full or incremental | Snowflake EXPORT → ADLS → COPY INTO | Large tables > 10GB |
# MAGIC
# MAGIC  ### clustering_type Reference
# MAGIC
# MAGIC  | clustering_type | cluster_by | Use When |
# MAGIC  |---|---|---|
# MAGIC  | `none` | NULL | Small tables, lookup tables |
# MAGIC  | `liquid` | column name(s) | Query filtering on those columns — Databricks recommended |
# MAGIC  | `partition` | single column | Very large tables with clear partition boundary |
