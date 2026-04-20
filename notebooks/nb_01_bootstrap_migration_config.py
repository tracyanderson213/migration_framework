# Databricks notebook source
# ============================================================
# nb_01_bootstrap_migration_config
# Reads SOURCE_TO_TARGET_DATASET from Snowflake via foreign
# catalog and populates table_migration_config.
#
# Auto-assigns process_group and load_mode by table size:
#   APPROX_SIZE_GB > threshold → {DB}_copy_into | copy_into
#   APPROX_SIZE_GB <= threshold → {DB}_federated | overwrite
#
# All rows inserted with enabled = false.
# Skips duplicates (src_database + src_schema + src_table).
# ============================================================

# COMMAND ----------

dbutils.widgets.text("admin_catalog",        "it")
dbutils.widgets.text("config_schema",        "migration_config")
dbutils.widgets.text("snowflake_catalog",    "sf_edw")
dbutils.widgets.text("source_db",            "EDW")
dbutils.widgets.text("source_schema",        "")
dbutils.widgets.text("target_catalog",       "it")
dbutils.widgets.text("target_schema",        "tpch_bronze")
dbutils.widgets.text("src_secret_scope",     "sf_scope")
dbutils.widgets.text("copy_into_size_gb",    "10")
dbutils.widgets.text("adls_stage",           "SANDBOX.DBX_MIGRATION_PRIVATE.ADLS_MIGRATION_STAGE")

ADMIN_CATALOG     = dbutils.widgets.get("admin_catalog")
CONFIG_SCHEMA     = dbutils.widgets.get("config_schema")
SF_CATALOG        = dbutils.widgets.get("snowflake_catalog")
SOURCE_DB         = dbutils.widgets.get("source_db").upper()
SOURCE_SCHEMA     = dbutils.widgets.get("source_schema").upper()
TARGET_CATALOG    = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA     = dbutils.widgets.get("target_schema")
SECRET_SCOPE      = dbutils.widgets.get("src_secret_scope")
COPY_INTO_SIZE_GB = float(dbutils.widgets.get("copy_into_size_gb") or "10")
ADLS_STAGE        = dbutils.widgets.get("adls_stage")
CONFIG_TABLE      = f"{ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config"

print(f"Source          : {SOURCE_DB}.{SOURCE_SCHEMA or '(all schemas)'}")
print(f"SF Catalog      : {SF_CATALOG}")
print(f"Target          : {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Copy_into >= GB : {COPY_INTO_SIZE_GB}")
print(f"Config table    : {CONFIG_TABLE}")

# COMMAND ----------

schema_filter = f"AND SOURCE_SCHEMA = '{SOURCE_SCHEMA}'" if SOURCE_SCHEMA else ""

source_df = spark.sql(f"""
    SELECT
        SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_OBJECT_NAME,
        OBJECT_TYPE, TARGET_CATALOG, TARGET_SCHEMA,
        TARGET_OBJECT_NAME, LOAD_STRATEGY,
        APPROX_SIZE_GB, CLUSTER_BY_COLS, ASSET_ID
    FROM {SF_CATALOG}.DBX_MIGRATION_PRIVATE.SOURCE_TO_TARGET_DATASET
    WHERE UPPER(SOURCE_DATABASE) = '{SOURCE_DB}'
      AND UPPER(ACTIVE_IND)      = 'TRUE'
      AND OBJECT_TYPE            = 'TABLE'
      {schema_filter}
    ORDER BY SOURCE_SCHEMA, ASSET_ID
""")

total_source = source_df.count()
print(f"Found {total_source} active TABLE rows in SOURCE_TO_TARGET_DATASET")

# COMMAND ----------

existing_keys = set(
    row['key'] for row in spark.sql(f"""
        SELECT UPPER(src_database)||'.'||UPPER(src_schema)||'.'||UPPER(src_table) AS key
        FROM {CONFIG_TABLE} WHERE source_type = 'snowflake'
    """).collect()
)
print(f"Existing rows in config: {len(existing_keys)}")

# COMMAND ----------

source_rows      = source_df.collect()
inserted         = 0
skipped          = 0
federated_count  = 0
copy_into_count  = 0
errors           = []

for row in source_rows:
    key = f"{row['SOURCE_DATABASE'].upper()}.{row['SOURCE_SCHEMA'].upper()}.{row['SOURCE_OBJECT_NAME'].upper()}"
    if key in existing_keys:
        skipped += 1
        continue

    # ── Auto-assign load_mode and process_group by size ───────
    approx_gb = row['APPROX_SIZE_GB']
    use_copy  = approx_gb is not None and float(approx_gb) > COPY_INTO_SIZE_GB

    load_mode     = 'copy_into' if use_copy else 'overwrite'
    load_strategy = 'full'
    process_group = f"{SOURCE_DB}_copy_into" if use_copy else f"{SOURCE_DB}_federated"
    # copy_into runs sequentially (priority reflects load order)
    # federated runs in parallel (all priority 1 by default)
    priority      = 1

    # Clustering from SOURCE_TO_TARGET_DATASET guidance
    cluster_by_cols = row['CLUSTER_BY_COLS'] or ''
    if cluster_by_cols in ('Recommended', 'Required'):
        clustering_type = 'liquid'
        cluster_by      = ''  # engineer fills in actual columns
    else:
        clustering_type = 'none'
        cluster_by      = ''

    # Target names
    tgt_catalog = row['TARGET_CATALOG'] or TARGET_CATALOG
    tgt_catalog = TARGET_CATALOG if tgt_catalog == 'TBD' else tgt_catalog
    tgt_schema  = row['TARGET_SCHEMA']  or TARGET_SCHEMA
    tgt_table   = (row['TARGET_OBJECT_NAME'] or row['SOURCE_OBJECT_NAME']).lower()
    table_id    = f"{row['SOURCE_DATABASE'].lower()}_{row['SOURCE_SCHEMA'].lower()}_{row['SOURCE_OBJECT_NAME'].lower()}_{row['ASSET_ID']}"
    size_note   = f"{approx_gb:.2f} GB" if approx_gb else "unknown size"
    route_note  = f"Auto-routed to {process_group} ({load_mode}). Size: {size_note}. Cluster guidance: {cluster_by_cols or 'none'}. Asset: {row['ASSET_ID']}. Review before enabling."

    try:
        spark.sql(f"""
            INSERT INTO {CONFIG_TABLE}
            (table_id, source_type, src_database, src_schema, src_table,
             target_catalog, target_schema, target_table,
             load_strategy, load_mode,
             incremental_col, incremental_col_type, last_loaded_value,
             batch_filter, primary_keys, merge_keys,
             clustering_type, cluster_by, partition_col,
             process_group, process_subgroup, depends_on_group,
             src_secret_scope, enabled, priority,
             last_run_status, last_run_at,
             last_sf_row_count, last_delta_count, notes)
            VALUES (
                '{table_id}', 'snowflake',
                '{row['SOURCE_DATABASE']}', '{row['SOURCE_SCHEMA']}', '{row['SOURCE_OBJECT_NAME']}',
                '{tgt_catalog}', '{tgt_schema}', '{tgt_table}',
                '{load_strategy}', '{load_mode}',
                NULL, 'none', NULL, NULL, NULL, NULL,
                '{clustering_type}', '{cluster_by}', NULL,
                '{process_group}', '{row['SOURCE_SCHEMA']}', NULL,
                '{SECRET_SCOPE}', false, {priority},
                'PENDING', NULL, NULL, NULL,
                '{route_note.replace("'", "''")}'
            )
        """)
        existing_keys.add(key)
        inserted += 1
        if use_copy:
            copy_into_count += 1
        else:
            federated_count += 1
    except Exception as e:
        errors.append(f"{key}: {str(e)[:200]}")

print(f"\nBootstrap complete")
print(f"  Inserted    : {inserted}")
print(f"   → federated  : {federated_count}  (process_group: {SOURCE_DB}_federated)")
print(f"   → copy_into  : {copy_into_count}  (process_group: {SOURCE_DB}_copy_into)")
print(f"  Skipped     : {skipped}")
print(f"  Errors      : {len(errors)}")
if errors:
    for e in errors: print(f"  ERROR: {e}")

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_id, src_schema, src_table,
           load_mode, process_group, priority,
           clustering_type, cluster_by,
           enabled, last_run_status, notes
    FROM {CONFIG_TABLE}
    WHERE src_database = '{SOURCE_DB}'
    ORDER BY process_group, priority, src_schema, src_table
"""))

# COMMAND ----------

print(f"""
All rows inserted with enabled = false.

Before running workflows:
  1. Review {SOURCE_DB}_federated rows
     - Set primary_keys and merge_keys for any incremental tables
     - Set incremental_col / incremental_col_type if needed
     - Set cluster_by columns where clustering_type = liquid
     - Adjust priority for dependency ordering

  2. Review {SOURCE_DB}_copy_into rows
     - Confirm load_mode = copy_into is correct
     - Set priority to control sequential execution order
     - Ensure ADLS stage is registered in Snowflake: {ADLS_STAGE}
     - Confirm Snowflake JDBC driver on cluster

  3. Set enabled = true for tables ready to run

  4. Set max_workers = 1 on {SOURCE_DB}_copy_into workflow
     to enforce sequential execution
""")
