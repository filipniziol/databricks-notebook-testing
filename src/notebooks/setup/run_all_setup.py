# Databricks notebook source
# MAGIC %md
# MAGIC # Master Setup Notebook
# MAGIC This notebook iterates through all numbered setup notebooks and executes them in order.

# COMMAND ----------

import os

# Get the current notebook path
current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
setup_folder = "/".join(current_path.split("/")[:-1])

print(f"Setup folder: {setup_folder}")

# COMMAND ----------

# List all notebooks in the setup folder
notebooks = []
for file_info in dbutils.fs.ls(f"file:/Workspace{setup_folder}"):
    name = file_info.name
    # Skip this master notebook and get only numbered notebooks (3-digit prefix)
    if name == "run_all_setup" or name == "run_all_setup.py":
        continue
    if len(name) >= 3 and name[0:3].isdigit() and name.endswith(".sql"):
        notebooks.append(name.replace(".sql", ""))

# Sort by numeric prefix
notebooks.sort(key=lambda x: int(x.split("_")[0]))
print(f"Found {len(notebooks)} setup notebooks to run:")
for nb in notebooks:
    print(f"  - {nb}")

# COMMAND ----------

# Execute each notebook in order
for notebook_name in notebooks:
    notebook_path = f"{setup_folder}/{notebook_name}"
    print(f"\n{'='*60}")
    print(f"Running: {notebook_name}")
    print(f"{'='*60}")
    
    try:
        result = dbutils.notebook.run(notebook_path, timeout_seconds=300)
        print(f"✓ Completed: {notebook_name}")
        if result:
            print(f"  Result: {result}")
    except Exception as e:
        print(f"✗ Failed: {notebook_name}")
        print(f"  Error: {str(e)}")
        raise

# COMMAND ----------

print("\n" + "="*60)
print("All setup notebooks completed successfully!")
print("="*60)
