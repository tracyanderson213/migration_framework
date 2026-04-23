# Databricks notebook source
# ============================================================
# nb_run_workflow
# POC convenience notebook — runs the three workflow notebooks
# sequentially in a single interactive session.
#
# Assumes this notebook lives in the same folder as:
#   nb_02_workflow_init
#   nb_03_dispatcher
#   nb_04_workflow_summary
#
# The folder path is resolved automatically from the current
# notebook location — no hardcoded paths needed.
#
# Not intended for production use — use a Databricks Workflow
# for scheduled or automated runs.
# ============================================================

# COMMAND ----------

dbutils.widgets.text("process_group",  "TPCH_foreign_catalog")
dbutils.widgets.text("admin_catalog",  "it")
dbutils.widgets.text("config_schema",  "migration_config")
dbutils.widgets.text("max_workers",    "8")
dbutils.widgets.text("adls_base_path", "abfss://migration@storageaccount.dfs.core.windows.net")

PROCESS_GROUP = dbutils.widgets.get("process_group")
ADMIN_CATALOG = dbutils.widgets.get("admin_catalog")
CONFIG_SCHEMA = dbutils.widgets.get("config_schema")
MAX_WORKERS   = dbutils.widgets.get("max_workers")
ADLS_BASE     = dbutils.widgets.get("adls_base_path")

# ── Resolve folder path from current notebook location ────────
# Strips the notebook filename from the full path leaving
# just the folder. Works regardless of where notebooks live.
NOTEBOOK_BASE = dbutils.notebook.entry_point \
    .getDbutils().notebook().getContext() \
    .notebookPath().get() \
    .rsplit("/", 1)[0]

# Shared parameters passed to every notebook
params = {
    "process_group"  : PROCESS_GROUP,
    "admin_catalog"  : ADMIN_CATALOG,
    "config_schema"  : CONFIG_SCHEMA,
    "max_workers"    : MAX_WORKERS,
    "adls_base_path" : ADLS_BASE,
}

print(f"Process group  : {PROCESS_GROUP}")
print(f"Admin catalog  : {ADMIN_CATALOG}")
print(f"Notebook folder: {NOTEBOOK_BASE}")

# COMMAND ----------

# ============================================================
# Task 1: init 
# ============================================================
# nb_02_workflow_init
# First task in every migration workflow.
# Validates config, marks enabled tables as RUNNING,
# and emits priority tier information for the dispatcher.

print("="*60)
print("TASK 1 — nb_02_workflow_init")
print("="*60)

result_init = dbutils.notebook.run(
    f"{NOTEBOOK_BASE}/nb_02_workflow_init",
    timeout_seconds = 300,
    arguments       = params
)
print(f"Init result: {result_init}")

# COMMAND ----------

# ============================================================
# Task 2: dispatch 
# ============================================================
# nb_03_dispatcher
# Core execution engine for the migration workflow.
#
# Reads table_migration_config for the given process_group,
# groups tables by priority tier, then:
#   - Runs tiers SEQUENTIALLY (tier 1 completes before tier 2)
#   - Within each tier runs tables IN PARALLEL via ThreadPool
#
# Routes each table by load_mode:
#   overwrite  → load_full_overwrite()   foreign catalog read
#   append     → load_full_append()      foreign catalog read
#   merge      → load_incremental_merge() foreign catalog read + MERGE INTO
#   copy_into  → load_copy_into()        JDBC export Snowflake→ADLS(Parquet) + COPY INTO Delta
#


print("="*60)
print("TASK 2 — nb_03_dispatcher")
print("="*60)

result_dispatch = dbutils.notebook.run(
    f"{NOTEBOOK_BASE}/nb_03_dispatcher",
    timeout_seconds = 3600,
    arguments       = params
)
print(f"Dispatch result: {result_dispatch}")

# COMMAND ----------

# ============================================================
# Task 3: summary 
# ============================================================
# nb_04_workflow_summary
# Final task in every migration workflow.
# Reads final run status for all tables in the process_group,
# prints a summary report, and raises an exception if any
# table FAILED so the Databricks Workflow shows as failed.
print("="*60)
print("TASK 3 — nb_04_workflow_summary")
print("="*60)

result_summary = dbutils.notebook.run(
    f"{NOTEBOOK_BASE}/nb_04_workflow_summary",
    timeout_seconds = 300,
    arguments       = params
)
print(f"Summary result: {result_summary}")

# COMMAND ----------

print(f"\n{'='*60}")
print(f"RUN COMPLETE — {PROCESS_GROUP}")
print(f"{'='*60}")
print(f"Init     : {result_init}")
print(f"Dispatch : {result_dispatch}")
print(f"Summary  : {result_summary}")

# COMMAND ----------

# MAGIC %md
# MAGIC **STOP HERE**

# COMMAND ----------

dbutils.notebook.exit("Stopped here for debugging")

# COMMAND ----------

# DBTITLE 1,Reset Tables
# COMMAND ----------

# ── OPTIONAL: Truncate target tables before run (testing only) ─
# Drops all Delta tables for the given process group so the
# next run loads from scratch. Useful for resetting POC state.
# Set TRUNCATE_TABLES = True to enable — False by default.

TRUNCATE_TABLES = False  # ← change to True to reset before run
# TRUNCATE_TABLES = True  # ← change to True to reset before run

if TRUNCATE_TABLES:
    rows = spark.sql(f"""
        SELECT target_catalog, target_schema, target_table
        FROM {ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config
        WHERE process_group = '{PROCESS_GROUP}'
          AND enabled       = true
    """).collect()

    print(f"Truncating {len(rows)} tables for process_group='{PROCESS_GROUP}'")
    for row in rows:
        tgt = f"{row['target_catalog']}.{row['target_schema']}.{row['target_table']}"
        try:
            spark.sql(f"TRUNCATE TABLE {tgt}")
            print(f"  Truncated : {tgt}")
        except Exception as e:
            print(f"  Skipped   : {tgt} — {str(e)[:80]}")

    # Reset config table watermarks and status
    spark.sql(f"""
        UPDATE {ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config
        SET last_run_status   = 'PENDING',
            last_loaded_value  = NULL,
            last_sf_row_count  = NULL,
            last_delta_count   = NULL,
            notes              = NULL
        WHERE process_group = '{PROCESS_GROUP}'
          AND enabled       = true
    """)
    print(f"Config table reset to PENDING for all rows")
else:
    print("TRUNCATE_TABLES = False — skipping truncation")
    print("Set TRUNCATE_TABLES = True to reset tables before run")

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC         SELECT * FROM ${admin_catalog}.${config_schema}.table_migration_config
# MAGIC          WHERE process_group = '${process_group}'
