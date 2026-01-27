# Copilot Instructions for poker_analyzer

## General Rules

### Language
- **All code, comments, and documentation must be in English**
- Variable names, function names, class names - English only
- Commit messages - English
- README, docstrings - English

## Databricks Asset Bundle Tips

### Naming Conventions
- Setup notebooks use 3-digit prefix: `{nnn}_{action}_{object}.sql` - this is our **migration framework**
- Examples: `001_create_catalog_poker.sql`, `002_create_schema_bronze.sql`
- Master notebook `run_all_setup.py` executes all numbered notebooks in order
- New migrations = new numbered notebook (e.g., `005_create_table_xyz.sql`)
- Never modify existing migration numbers - always add new ones

### One Object Per Migration (CRITICAL)
- **Each migration file should create exactly ONE object** (1 table, 1 view, 1 schema)
- Never put multiple CREATE statements in one migration
- This makes tracking, debugging, and re-running easier
- Bad: `015_create_views_gold.sql` with 4 views
- Good: `015_create_view_tournament_analysis.sql`, `016_create_view_tournament_summary_by_stage.sql`

### SQL Notebook Header (CRITICAL)
- **EVERY .sql file MUST start with `-- Databricks notebook source`**
- Without this header, Databricks cannot run the file as a notebook
- Format:
```sql
-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Table: my_table
-- MAGIC Description here

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ...
```

### SQL vs Python for DDL
- Use `.sql` files for schema/catalog/volume creation (DDL operations)
- Use `.py` files only when Python logic is needed
- SQL notebooks are cleaner and more readable for infrastructure setup

### Unity Catalog Structure
- Pattern: `{catalog}.{schema}.{object}`
- Use 1 volume per schema, not separate volumes per data type
- Volume naming: `poker.{schema}.{schema}` (e.g., `poker.bronze.bronze`)
- Use subfolders inside volumes for different data types:
  - `/Volumes/poker/bronze/bronze/analysis_json/`
  - `/Volumes/poker/bronze/bronze/gg_hand_history/`

### Serverless Configuration
- Define `environment_version` as a variable in `databricks.yml`
- Reference with `${var.serverless_environment_version}`
- Correct syntax for environments in jobs:
```yaml
environments:
  - environment_key: serverless
    spec:
      environment_version: "${var.serverless_environment_version}"
```

### Job Design
- Use a single master notebook that iterates through numbered notebooks
- Avoid creating separate tasks for each notebook (doesn't scale)
- Master notebook should use `dbutils.notebook.run()` to execute child notebooks in order

### Resource File Naming
- **YAML file name MUST match the job/pipeline key name**
- Example: job `bronze_ingestion` → file `resources/bronze_ingestion.yml`
- Example: job `poker_analyzer_init` → file `resources/poker_analyzer_init.yml`
- This keeps the codebase consistent and easy to navigate

### Project Structure
```
poker_analyzer/
├── databricks.yml
├── resources/
│   └── *.yml          # Job definitions
└── src/
    ├── notebooks/
    │   ├── setup/     # Infrastructure notebooks (SQL)
    │   ├── bronze/    # Raw data ingestion
    │   └── silver/    # Parsed/transformed data
    └── package/       # Python modules for imports
```

### Variables
- Always use variables for values that might change between environments
- Define in `databricks.yml` under `variables:` with defaults
- Override per target if needed

### Notebook Best Practices

#### No verification/display cells in production notebooks
- **NEVER** add "verify results" or summary SQL queries at the end
- Production notebooks should do ONE thing: transform and save
- Debug/verify cells belong in separate ad-hoc notebooks, not in jobs

#### Avoid count() for control flow
- **NEVER** use `df.count()` to check if dataframe is empty before writing
- `count()` triggers full computation and is slow
- Just write the data - if empty, nothing happens
- Bad: `if df.count() > 0: df.write...`
- Good: `df.write...`

#### Small tables: full overwrite
- For small reference tables (< 1M rows), use `mode("overwrite")`
- Simpler logic, no deduplication needed
- Always consistent state
- Example: tournament results, player stats

#### Large tables: streaming or incremental
- Use `readStream` + `writeStream` with checkpoints
- Or MERGE INTO for upserts
- Track what's been processed via watermarks or checkpoints
