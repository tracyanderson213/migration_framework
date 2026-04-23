# Databricks notebook source
# ============================================================
# nb_00_setup
# One-time setup notebook.
# Creates table_migration_config in Unity Catalog and inserts
# all POC rows in initial state (enabled=false, PENDING).
#
# Tables included:
#   TPCH_foreign_catalog process group (samples.tpch):
#     customer, orders, region, nation, supplier,
#     part, partsupp, lineitem
#   TPCH_autoloader process group (UC Volume CSV):
#     supplier (autoloader), nation (autoloader)
#   PG_NEON_jdbc process group (Neon PostgreSQL):
#     supplier_pg
#
# All rows inserted with:
#   enabled          = false  (engineer enables before first run)
#   last_run_status  = PENDING
#   last_loaded_value = NULL  (incremental watermark reset)
#   last_run_at      = NULL
#   last_sf_row_count = NULL
#   last_delta_count  = NULL
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
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config (
  table_id              STRING    COMMENT 'Unique ID for this migration row',
  source_type           STRING    COMMENT 'snowflake | sqlserver | postgresql | redshift | oracle | delta | adls',
  src_database          STRING    COMMENT 'Source database name or foreign catalog instance identifier',
  src_schema            STRING    COMMENT 'Source schema name',
  src_table             STRING    COMMENT 'Source table name',
  target_catalog        STRING    COMMENT 'Unity Catalog target catalog',
  target_schema         STRING    COMMENT 'Unity Catalog target schema',
  target_table          STRING    COMMENT 'Delta target table name',
  load_strategy         STRING    COMMENT 'full | incremental',
  load_mode             STRING    COMMENT 'overwrite | append | merge | copy_into | autoloader',
  incremental_col       STRING    COMMENT 'Watermark column name — nullable',
  incremental_col_type  STRING    COMMENT 'timestamp | date | integer | none',
  last_loaded_value     STRING    COMMENT 'High watermark from last successful run',
  batch_filter          STRING    COMMENT 'Optional WHERE clause for full load',
  primary_keys          STRING    COMMENT 'Comma-separated primary key columns',
  merge_keys            STRING    COMMENT 'Comma-separated columns for MERGE INTO join condition',
  clustering_type       STRING    COMMENT 'liquid | partition | none',
  cluster_by            STRING    COMMENT 'Comma-separated columns for liquid clustering',
  partition_col         STRING    COMMENT 'Partition column (clustering_type=partition only)',
  process_group         STRING    COMMENT 'Logical group — maps to a Databricks Workflow',
  process_subgroup      STRING    COMMENT 'Optional sub-group within a process group',
  depends_on_group      STRING    COMMENT 'process_group that must complete first — nullable',
  src_secret_scope      STRING    COMMENT 'Databricks secret scope — nullable for foreign catalog reads',
  enabled               BOOLEAN   COMMENT 'Default false — must be set true before workflow runs',
  priority              INT       COMMENT 'Run order within process group (lower = first)',
  last_run_status       STRING    COMMENT 'PENDING | RUNNING | PASSED | FAILED',
  last_run_at           TIMESTAMP COMMENT 'Timestamp of last run',
  last_sf_row_count     BIGINT    COMMENT 'Rows read from source on last run',
  last_delta_count      BIGINT    COMMENT 'Rows in Delta after last run',
  file_format           STRING    COMMENT 'parquet | json | csv — for Autoloader and ADLS loads',
  autoloader_mode       STRING    COMMENT 'availableNow | continuous — Autoloader trigger mode',
  schema_hints          STRING    COMMENT 'Optional schema hints for Autoloader',
  notes                 STRING    COMMENT 'Free-text notes and error messages for engineers'
)
USING DELTA
COMMENT 'Central config and run-state table for the migration framework'
""")
print(" table_migration_config created")

# COMMAND ----------

# ── Insert all POC rows ───────────────────────────────────────
# All rows inserted in initial state:
#   enabled = false, last_run_status = PENDING
#   all run-state columns = NULL
#
# Process groups:
#   TPCH_foreign_catalog — samples.tpch tables via foreign catalog
#   TPCH_autoloader      — CSV files in UC Volume via Autoloader
#   PG_NEON_jdbc         — Neon PostgreSQL via foreign catalog

rows = [

    # ── TPCH_foreign_catalog — priority 1 ────────────────────
    {
        'table_id'           : 'tpch_region_001',
        'source_type'        : 'snowflake',
        'src_database'       : 'samples',
        'src_schema'         : 'tpch',
        'src_table'          : 'region',
        'target_catalog'     : 'it',
        'target_schema'      : 'tpch_bronze',
        'target_table'       : 'tpch_region',
        'load_strategy'      : 'full',
        'load_mode'          : 'overwrite',
        'incremental_col'    : None,
        'incremental_col_type': 'none',
        'primary_keys'       : 'r_regionkey',
        'merge_keys'         : 'r_regionkey',
        'clustering_type'    : 'none',
        'cluster_by'         : None,
        'process_group'      : 'TPCH_foreign_catalog',
        'process_subgroup'   : 'tpch',
        'src_secret_scope'   : None,
        'priority'           : 1,
        'file_format'        : None,
        'autoloader_mode'    : None,
        'notes'              : 'Tiny reference table. 5 rows. Loads first.',
    },
    {
        'table_id'           : 'tpch_customer_001',
        'source_type'        : 'snowflake',
        'src_database'       : 'samples',
        'src_schema'         : 'tpch',
        'src_table'          : 'customer',
        'target_catalog'     : 'it',
        'target_schema'      : 'tpch_bronze',
        'target_table'       : 'customer',
        'load_strategy'      : 'full',
        'load_mode'          : 'overwrite',
        'incremental_col'    : None,
        'incremental_col_type': 'none',
        'primary_keys'       : 'c_custkey',
        'merge_keys'         : 'c_custkey',
        'clustering_type'    : 'none',
        'cluster_by'         : None,
        'process_group'      : 'TPCH_foreign_catalog',
        'process_subgroup'   : 'tpch',
        'src_secret_scope'   : None,
        'priority'           : 1,
        'file_format'        : None,
        'autoloader_mode'    : None,
        'notes'              : 'Full overwrite. ~750K rows.',
    },

    # ── TPCH_foreign_catalog — priority 2 ────────────────────
    {
        'table_id'           : 'tpch_nation_001',
        'source_type'        : 'snowflake',
        'src_database'       : 'samples',
        'src_schema'         : 'tpch',
        'src_table'          : 'nation',
        'target_catalog'     : 'it',
        'target_schema'      : 'tpch_bronze',
        'target_table'       : 'tpch_nation',
        'load_strategy'      : 'full',
        'load_mode'          : 'overwrite',
        'incremental_col'    : None,
        'incremental_col_type': 'none',
        'primary_keys'       : 'n_nationkey',
        'merge_keys'         : 'n_nationkey',
        'clustering_type'    : 'none',
        'cluster_by'         : None,
        'process_group'      : 'TPCH_foreign_catalog',
        'process_subgroup'   : 'tpch',
        'src_secret_scope'   : None,
        'priority'           : 2,
        'file_format'        : None,
        'autoloader_mode'    : None,
        'notes'              : 'Reference table. 25 rows.',
    },
    {
        'table_id'           : 'tpch_orders_001',
        'source_type'        : 'snowflake',
        'src_database'       : 'samples',
        'src_schema'         : 'tpch',
        'src_table'          : 'orders',
        'target_catalog'     : 'it',
        'target_schema'      : 'tpch_bronze',
        'target_table'       : 'orders',
        'load_strategy'      : 'incremental',
        'load_mode'          : 'merge',
        'incremental_col'    : 'o_orderdate',
        'incremental_col_type': 'date',
        'primary_keys'       : 'o_orderkey',
        'merge_keys'         : 'o_orderkey',
        'clustering_type'    : 'liquid',
        'cluster_by'         : 'o_orderdate',
        'process_group'      : 'TPCH_foreign_catalog',
        'process_subgroup'   : 'tpch',
        'src_secret_scope'   : None,
        'priority'           : 2,
        'file_format'        : None,
        'autoloader_mode'    : None,
        'notes'              : 'Incremental merge on o_orderdate. ~7.5M rows. Liquid clustering.',
    },

    # ── TPCH_foreign_catalog — priority 3 ────────────────────
    {
        'table_id'           : 'tpch_supplier_001',
        'source_type'        : 'snowflake',
        'src_database'       : 'samples',
        'src_schema'         : 'tpch',
        'src_table'          : 'supplier',
        'target_catalog'     : 'it',
        'target_schema'      : 'tpch_bronze',
        'target_table'       : 'tpch_supplier',
        'load_strategy'      : 'full',
        'load_mode'          : 'overwrite',
        'incremental_col'    : None,
        'incremental_col_type': 'none',
        'primary_keys'       : 's_suppkey',
        'merge_keys'         : 's_suppkey',
        'clustering_type'    : 'none',
        'cluster_by'         : None,
        'process_group'      : 'TPCH_foreign_catalog',
        'process_subgroup'   : 'tpch',
        'src_secret_scope'   : None,
        'priority'           : 3,
        'file_format'        : None,
        'autoloader_mode'    : None,
        'notes'              : 'Full overwrite. ~50K rows.',
    },
    {
        'table_id'           : 'tpch_part_001',
        'source_type'        : 'snowflake',
        'src_database'       : 'samples',
        'src_schema'         : 'tpch',
        'src_table'          : 'part',
        'target_catalog'     : 'it',
        'target_schema'      : 'tpch_bronze',
        'target_table'       : 'tpch_part',
        'load_strategy'      : 'full',
        'load_mode'          : 'overwrite',
        'incremental_col'    : None,
        'incremental_col_type': 'none',
        'primary_keys'       : 'p_partkey',
        'merge_keys'         : 'p_partkey',
        'clustering_type'    : 'none',
        'cluster_by'         : None,
        'process_group'      : 'TPCH_foreign_catalog',
        'process_subgroup'   : 'tpch',
        'src_secret_scope'   : None,
        'priority'           : 3,
        'file_format'        : None,
        'autoloader_mode'    : None,
        'notes'              : 'Full overwrite. ~1M rows. Independent of supplier.',
    },

    # ── TPCH_foreign_catalog — priority 4 ────────────────────
    {
        'table_id'           : 'tpch_partsupp_001',
        'source_type'        : 'snowflake',
        'src_database'       : 'samples',
        'src_schema'         : 'tpch',
        'src_table'          : 'partsupp',
        'target_catalog'     : 'it',
        'target_schema'      : 'tpch_bronze',
        'target_table'       : 'tpch_partsupp',
        'load_strategy'      : 'full',
        'load_mode'          : 'overwrite',
        'incremental_col'    : None,
        'incremental_col_type': 'none',
        'primary_keys'       : 'ps_partkey,ps_suppkey',
        'merge_keys'         : 'ps_partkey,ps_suppkey',
        'clustering_type'    : 'liquid',
        'cluster_by'         : 'ps_partkey',
        'process_group'      : 'TPCH_foreign_catalog',
        'process_subgroup'   : 'tpch',
        'src_secret_scope'   : None,
        'priority'           : 4,
        'file_format'        : None,
        'autoloader_mode'    : None,
        'notes'              : 'Composite PK. ~4M rows. Runs after part and supplier.',
    },

    # ── TPCH_foreign_catalog — priority 5 ────────────────────
    {
        'table_id'           : 'tpch_lineitem_001',
        'source_type'        : 'snowflake',
        'src_database'       : 'samples',
        'src_schema'         : 'tpch',
        'src_table'          : 'lineitem',
        'target_catalog'     : 'it',
        'target_schema'      : 'tpch_bronze',
        'target_table'       : 'tpch_lineitem',
        'load_strategy'      : 'incremental',
        'load_mode'          : 'merge',
        'incremental_col'    : 'l_shipdate',
        'incremental_col_type': 'date',
        'primary_keys'       : 'l_orderkey,l_linenumber',
        'merge_keys'         : 'l_orderkey,l_linenumber',
        'clustering_type'    : 'liquid',
        'cluster_by'         : 'l_shipdate',
        'process_group'      : 'TPCH_foreign_catalog',
        'process_subgroup'   : 'tpch',
        'src_secret_scope'   : None,
        'priority'           : 5,
        'file_format'        : None,
        'autoloader_mode'    : None,
        'notes'              : 'Largest table. ~30M rows. Composite PK. Incremental on l_shipdate.',
    },

    # ── TPCH_autoloader — UC Volume CSV files ─────────────────
    {
        'table_id'           : 'tpch_supplier_adls_001',
        'source_type'        : 'adls',
        'src_database'       : 'it',
        'src_schema'         : 'tpch_bronze',
        'src_table'          : 'supplier',
        'target_catalog'     : 'it',
        'target_schema'      : 'tpch_bronze',
        'target_table'       : 'tpch_supplier_autoloader',
        'load_strategy'      : 'incremental',
        'load_mode'          : 'autoloader',
        'incremental_col'    : None,
        'incremental_col_type': 'none',
        'primary_keys'       : 's_suppkey',
        'merge_keys'         : None,
        'clustering_type'    : 'none',
        'cluster_by'         : None,
        'process_group'      : 'TPCH_autoloader',
        'process_subgroup'   : 'tpch',
        'src_secret_scope'   : None,
        'priority'           : 1,
        'file_format'        : 'csv',
        'autoloader_mode'    : 'availableNow',
        'notes'              : 'Autoloader CSV from /Volumes/it/tpch_bronze/landing/supplier/',
    },
    {
        'table_id'           : 'tpch_nation_adls_001',
        'source_type'        : 'adls',
        'src_database'       : 'it',
        'src_schema'         : 'tpch_bronze',
        'src_table'          : 'nation',
        'target_catalog'     : 'it',
        'target_schema'      : 'tpch_bronze',
        'target_table'       : 'tpch_nation_autoloader',
        'load_strategy'      : 'incremental',
        'load_mode'          : 'autoloader',
        'incremental_col'    : None,
        'incremental_col_type': 'none',
        'primary_keys'       : 'n_nationkey',
        'merge_keys'         : None,
        'clustering_type'    : 'none',
        'cluster_by'         : None,
        'process_group'      : 'TPCH_autoloader',
        'process_subgroup'   : 'tpch',
        'src_secret_scope'   : None,
        'priority'           : 2,
        'file_format'        : 'csv',
        'autoloader_mode'    : 'availableNow',
        'notes'              : 'Autoloader CSV from /Volumes/it/tpch_bronze/landing/nation/',
    },

    # ── PG_NEON_jdbc — Neon PostgreSQL ────────────────────────
    {
        'table_id'           : 'pg_neon_public_supplier_pg_001',
        'source_type'        : 'postgresql',
        'src_database'       : 'pg_neon',
        'src_schema'         : 'public',
        'src_table'          : 'supplier_pg',
        'target_catalog'     : 'it',
        'target_schema'      : 'tpch_bronze',
        'target_table'       : 'pg_neon_supplier_pg',
        'load_strategy'      : 'incremental',
        'load_mode'          : 'merge',
        'incremental_col'    : 'updated_at',
        'incremental_col_type': 'timestamp',
        'primary_keys'       : 's_suppkey',
        'merge_keys'         : 's_suppkey',
        'clustering_type'    : 'none',
        'cluster_by'         : None,
        'process_group'      : 'PG_NEON_jdbc',
        'process_subgroup'   : 'public',
        'src_secret_scope'   : 'pg_neon',
        'priority'           : 1,
        'file_format'        : None,
        'autoloader_mode'    : None,
        'notes'              : 'Neon PostgreSQL via foreign catalog pg_neon. Incremental merge on updated_at.',
    },
]

# ── Build and execute inserts ─────────────────────────────────
inserted = 0
skipped  = 0

# Get existing table_ids to avoid duplicates
existing = set(
    r['table_id'] for r in spark.sql(f"""
        SELECT table_id FROM {ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config
    """).collect()
)

for row in rows:
    if row['table_id'] in existing:
        print(f"  Skipped (exists): {row['table_id']}")
        skipped += 1
        continue

    def val(v):
        if v is None:
            return 'NULL'
        v = str(v).replace("'", "''")
        return f"'{v}'"

    spark.sql(f"""
        INSERT INTO {ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config
        VALUES (
            {val(row['table_id'])},
            {val(row['source_type'])},
            {val(row['src_database'])},
            {val(row['src_schema'])},
            {val(row['src_table'])},
            {val(row['target_catalog'])},
            {val(row['target_schema'])},
            {val(row['target_table'])},
            {val(row['load_strategy'])},
            {val(row['load_mode'])},
            {val(row.get('incremental_col'))},
            {val(row.get('incremental_col_type'))},
            NULL,
            NULL,
            {val(row.get('primary_keys'))},
            {val(row.get('merge_keys'))},
            {val(row.get('clustering_type','none'))},
            {val(row.get('cluster_by'))},
            NULL,
            {val(row['process_group'])},
            {val(row.get('process_subgroup'))},
            NULL,
            {val(row.get('src_secret_scope'))},
            false,
            {row['priority']},
            'PENDING',
            NULL,
            NULL,
            NULL,
            {val(row.get('file_format'))},
            {val(row.get('autoloader_mode'))},
            NULL,
            {val(row.get('notes'))}
        )
    """)
    print(f"  Inserted: {row['table_id']}")
    inserted += 1

print(f"\n Setup complete — Inserted: {inserted} | Skipped: {skipped}")

# COMMAND ----------

# ── Verify ────────────────────────────────────────────────────
display(spark.sql(f"""
    SELECT
        table_id,
        source_type,
        src_database || '.' || src_schema || '.' || src_table AS source,
        target_catalog || '.' || target_schema || '.' || target_table AS target,
        load_mode,
        incremental_col,
        primary_keys,
        process_group,
        priority,
        enabled,
        last_run_status
    FROM {ADMIN_CATALOG}.{CONFIG_SCHEMA}.table_migration_config
    ORDER BY process_group, priority, table_id
"""))

# COMMAND ----------

# ── Connection tests ──────────────────────────────────────────
print("Testing source connections...")
tests = [
    ("samples.tpch.customer",      "SELECT COUNT(*) AS cnt FROM samples.tpch.customer"),
    ("samples.tpch.orders",        "SELECT COUNT(*) AS cnt FROM samples.tpch.orders"),
    ("samples.tpch.lineitem",      "SELECT COUNT(*) AS cnt FROM samples.tpch.lineitem"),
    ("pg_neon.public.supplier_pg", "SELECT COUNT(*) AS cnt FROM pg_neon.public.supplier_pg"),
]

for label, sql in tests:
    try:
        cnt = spark.sql(sql).collect()[0]['cnt']
        print(f"  {label:<40} {cnt:>12,} rows  OK")
    except Exception as e:
        print(f"  {label:<40} FAILED — {str(e)[:80]}")

print("""
All rows inserted with enabled = false.
Review the table above then enable rows by process group:

  -- Enable TPCH foreign catalog tables
  UPDATE it.migration_config.table_migration_config
  SET enabled = true
  WHERE process_group = 'TPCH_foreign_catalog';

  -- Enable Autoloader tables (requires volume files to exist)
  UPDATE it.migration_config.table_migration_config
  SET enabled = true
  WHERE process_group = 'TPCH_autoloader';

  -- Enable Neon PostgreSQL (requires pg_neon foreign catalog)
  UPDATE it.migration_config.table_migration_config
  SET enabled = true
  WHERE process_group = 'PG_NEON_jdbc';
""")
