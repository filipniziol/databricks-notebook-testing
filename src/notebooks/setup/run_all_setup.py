# Databricks notebook source
# MAGIC %md
# MAGIC # Master Setup Notebook
# MAGIC This notebook iterates through all numbered setup notebooks and executes them in order.
# MAGIC 
# MAGIC **Features:**
# MAGIC - Tracks executed migrations in `poker.bronze._migrations`
# MAGIC - Skips already executed notebooks (incremental)
# MAGIC - Use `force_rerun=True` to re-execute all

# COMMAND ----------

import os
import time

# Configuration
FORCE_RERUN = False  # Set to True to re-run all migrations

# COMMAND ----------

# Get the current notebook path
current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
setup_folder = "/".join(current_path.split("/")[:-1])

print(f"Setup folder: {setup_folder}")
print(f"Force rerun: {FORCE_RERUN}")

# COMMAND ----------

# Ensure migrations table exists (bootstrap - run 000 first if needed)
try:
    spark.sql("SELECT 1 FROM poker.bronze._migrations LIMIT 1")
    print("✓ Migrations table exists")
except:
    print("⚠ Migrations table not found - will create it first")
    # Run 000 to create migrations table
    dbutils.notebook.run(f"{setup_folder}/000_create_migrations_table", timeout_seconds=60)
    print("✓ Created migrations table")

# COMMAND ----------

# Get already executed migrations
if not FORCE_RERUN:
    executed_df = spark.sql("""
        SELECT migration_name 
        FROM poker.bronze._migrations 
        WHERE status = 'SUCCESS'
    """)
    executed_set = set(row.migration_name for row in executed_df.collect())
    print(f"Already executed: {len(executed_set)} migrations")
else:
    executed_set = set()
    print("Force rerun mode - will execute all migrations")

# COMMAND ----------

# List all notebooks in the setup folder
notebooks = []
for name in os.listdir(f"/Workspace{setup_folder}"):
    # Skip this master notebook and get only numbered notebooks (3-digit prefix)
    if name == "run_all_setup" or name == "run_all_setup.py":
        continue
    if len(name) >= 3 and name[0:3].isdigit() and name.endswith(".sql"):
        notebooks.append(name.replace(".sql", ""))

# Sort by numeric prefix
notebooks.sort(key=lambda x: int(x.split("_")[0]))

# Filter out already executed
pending = [nb for nb in notebooks if nb not in executed_set]

print(f"\nFound {len(notebooks)} total notebooks")
print(f"Pending execution: {len(pending)}")
for nb in pending:
    print(f"  → {nb}")

if not pending:
    print("\n✅ All migrations already executed!")

# COMMAND ----------

# Execute pending notebooks
for notebook_name in pending:
    notebook_path = f"{setup_folder}/{notebook_name}"
    print(f"\n{'='*60}")
    print(f"Running: {notebook_name}")
    print(f"{'='*60}")
    
    start_time = time.time()
    status = "SUCCESS"
    
    try:
        result = dbutils.notebook.run(notebook_path, timeout_seconds=300)
        execution_time = time.time() - start_time
        print(f"✓ Completed: {notebook_name} ({execution_time:.1f}s)")
        if result:
            print(f"  Result: {result}")
    except Exception as e:
        execution_time = time.time() - start_time
        status = "FAILED"
        print(f"✗ Failed: {notebook_name} ({execution_time:.1f}s)")
        print(f"  Error: {str(e)}")
        
        # Record failure
        spark.sql(f"""
            INSERT INTO poker.bronze._migrations VALUES 
            ('{notebook_name}', current_timestamp(), {execution_time}, '{status}')
        """)
        raise
    
    # Record success
    spark.sql(f"""
        INSERT INTO poker.bronze._migrations VALUES 
        ('{notebook_name}', current_timestamp(), {execution_time}, '{status}')
    """)

# COMMAND ----------

print("\n" + "="*60)
print("All setup notebooks completed successfully!")
print("="*60)
