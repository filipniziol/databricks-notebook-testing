# Copilot Instructions for poker_analyzer

## General Rules

### Language
- **All code, comments, and documentation must be in English**
- Variable names, function names, class names - English only
- Commit messages - English
- README, docstrings - English

### Databricks Queries
- **Copilot cannot run SQL queries directly** - ask the user to execute them
- When debugging data issues, provide the SQL query and ask user to run it and share results
- Example: "Could you run this query and share the results? `SELECT * FROM poker.gold.player_statistics WHERE player_name = 'XYZ'`"

## Databricks Asset Bundle Tips

### Naming Conventions
- Setup notebooks use 3-digit prefix: `{nnn}_{action}_{object}.sql` - this is our **migration framework**
- Examples: `001_create_catalog_poker.sql`, `002_create_schema_bronze.sql`
- Master notebook `run_all_setup.py` executes all numbered notebooks in order
- New migrations = new numbered notebook (e.g., `005_create_table_xyz.sql`)
- Never modify existing migration numbers - always add new ones

### One Object Per Migration (CRITICAL)
- **Each migration file should create exactly ONE object** (1 table, 1 view, 1 schema)
- **1 VIEW = 1 FILE** - never put multiple views in one file
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

### Notebook Best Practices

#### No verification/display cells in production notebooks
- **NEVER** add "verify results" or summary SQL queries at the end
- Production notebooks should do ONE thing: transform and save
- Debug/verify cells belong in separate ad-hoc notebooks, not in jobs

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

---

## Setup Notebooks Reference (000-023)

All setup notebooks live in `src/notebooks/setup/` and run in numerical order via `run_all_setup.py`.

### Infrastructure (000-004)
| File | Object | Description |
|------|--------|-------------|
| `000_create_migrations_table.sql` | `poker.bronze._migrations` | Tracks which migrations have been executed (prevents re-runs) |
| `001_create_catalog_poker.sql` | `poker` catalog | Main catalog for all poker data |
| `002_create_schema_bronze.sql` | `poker.bronze` schema + volume | Raw data layer |
| `003_create_schema_silver.sql` | `poker.silver` schema + volume | Processed/matched data layer |
| `004_create_schema_gold.sql` | `poker.gold` schema + volume | Analytics/aggregations layer |

### Bronze Tables (005-007)
| File | Table | Description |
|------|-------|-------------|
| `005_create_table_bronze_analysis_result.sql` | `poker.bronze.analysis_result` | Raw JSON from screenshot analyzer |
| `006_create_table_bronze_hand_history.sql` | `poker.bronze.hand_history` | Raw text from GGPoker hand history |
| `007_create_table_bronze_tournament_history.sql` | `poker.bronze.tournament_history` | Raw text from GGPoker tournament results |

### Silver Tables (008-014, 019)
| File | Table | Description |
|------|-------|-------------|
| `008_create_table_silver_screenshots.sql` | `poker.silver.screenshots` | Main screenshot data with hero + GPT advice |
| `009_create_table_silver_screenshot_players.sql` | `poker.silver.screenshot_players` | Opponents per screenshot (1:N) |
| `010_create_table_silver_screenshot_history.sql` | `poker.silver.screenshot_history` | Hand history per street per screenshot |
| `011_create_table_silver_tournaments.sql` | `poker.silver.tournaments` | Parsed tournament results |
| `012_create_table_silver_hands.sql` | `poker.silver.hands` | Hand headers - one row per hand |
| `013_create_table_silver_hand_players.sql` | `poker.silver.hand_players` | Players per hand with VPIP, PFR, etc. |
| `014_create_table_silver_hand_actions.sql` | `poker.silver.hand_actions` | Individual actions per hand |
| `019_create_table_silver_screenshot_hand_mapping.sql` | `poker.silver.screenshot_hand_mapping` | Bridge: screenshots → hands (n:1) |

### Functions (020)
| File | Function | Description |
|------|----------|-------------|
| `020_create_poker_evaluation_functions.sql` | `poker.utils.evaluate_hand` | Evaluates hole cards + board → hand rank + name |
| | `poker.utils.compare_hands` | Compares hero vs array of opponents → winner |

### Gold Views - Tournament Analysis (015-018)
| File | View | Description |
|------|------|-------------|
| `015_create_view_gold_tournament_analysis.sql` | `poker.gold.tournament_analysis` | Tournament results with stage breakdown, bounty vs position |
| `016_create_view_gold_tournament_summary_by_stage.sql` | `poker.gold.tournament_summary_by_stage` | Stats aggregated by finish stage |
| `017_create_view_gold_tournament_summary_by_buyin.sql` | `poker.gold.tournament_summary_by_buyin` | Stats aggregated by buyin level ($10 vs $25) |
| `018_create_view_gold_tournament_summary_daily.sql` | `poker.gold.tournament_summary_daily` | Daily P&L tracking |

### Gold Views - GPT Analysis (021-023)
| File | View | Description |
|------|------|-------------|
| `021_create_view_gold_screenshot_hand_analysis.sql` | `poker.gold.screenshot_hand_analysis` | GPT advice vs actual outcome (did hero follow? was it profitable?) |
| `022_create_view_gold_hand_line_analysis.sql` | `poker.gold.hand_line_analysis` | Full hand line analysis - all GPT recs vs hero actions per hand |
| `023_create_view_gold_fold_showdown_analysis.sql` | `poker.gold.fold_showdown_analysis` | "What if" analysis - when hero folded but showdown happened |

### Gold Views - Behavioral Analysis (024-027)
| File | View | Description |
|------|------|-------------|
| `024_create_view_gold_gpt_compliance_stats.sql` | `poker.gold.gpt_compliance_stats` | Aggregated compliance by stage, street, stack depth, headsup vs multiway |
| `025_create_view_gold_deviation_outcome_analysis.sql` | `poker.gold.deviation_outcome_analysis` | What happens when hero deviates (all-in vs call, win rates) |
| `026_create_view_gold_tilt_detection.sql` | `poker.gold.tilt_detection` | Compliance after big losses, consecutive losses, session status |
| | `poker.gold.tilt_consecutive_losses` | Compliance by recent loss streak (0-1, 2-3, 4+ losses) |
| `027_create_view_gold_headsup_analysis.sql` | `poker.gold.headsup_analysis` | GPT compliance in heads-up vs multiway |
| | `poker.gold.headsup_fold_analysis` | Does GPT recommend fold too much in heads-up? |

---

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

---

## ML Development Best Practices

### Runtime Requirements
- **Feature Engineering requires ML Runtime** - serverless doesn't work
- Use `15.4.x-cpu-ml-scala2.12` or newer ML runtime
- `databricks-feature-engineering` package only available on ML clusters

### MLflow + Unity Catalog Constraints

#### Model Signatures (CRITICAL)
- **Unity Catalog requires model signature with BOTH input AND output**
- `infer_signature()` alone may not work - use `input_example` parameter
- Best approach: pass `input_example` to `log_model()` - MLflow auto-infers signature
```python
# Good - auto-infers both input and output
mlflow.sklearn.log_model(
    sk_model=model,
    artifact_path="model",
    input_example=X_train[:5],  # This triggers signature inference
    registered_model_name="catalog.schema.model_name"
)
```

#### No "latest" in Unity Catalog
- `models:/name/latest` doesn't work in Unity Catalog
- Use aliases: `models:/name@champion` (recommended)
- Or explicit version: `models:/name/3`
- Fallback pattern:
```python
try:
    model = mlflow.load_model(f"models:/{name}@champion")
except:
    versions = client.search_model_versions(f"name='{name}'")
    latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
    model = mlflow.load_model(f"models:/{name}/{latest.version}")
```

#### DECIMAL Types Don't Work
- MLflow signatures don't support Spark `DecimalType`
- Always cast to DOUBLE in SQL: `CAST(column AS DOUBLE)`
- Or convert in pandas: `df[col] = df[col].astype(float)`

### Feature Store Limitations

#### Unsupervised Learning
- `fe.log_model()` doesn't work well for unsupervised models (no label)
- Use regular `mlflow.sklearn.log_model()` instead
- You lose Feature Store lineage but model registers correctly

#### Feature Table Schema Changes
- If you change column types, you must DROP and recreate the table
- `fe.write_table(mode="merge")` won't change schema
```python
spark.sql(f"DROP TABLE IF EXISTS {FEATURE_TABLE_NAME}")
fe.create_table(name=..., primary_keys=[...], df=df_features)
```

### Notebook Separation (Production Pattern)

**Split ML pipelines into 3 notebooks:**

| Notebook | Purpose | Schedule |
|----------|---------|----------|
| `update_features.py` | Update Feature Table | Daily |
| `train_model.py` | Train & register model | Manual/Weekly |
| `score_batch.py` | Batch inference | Daily |

**Why separate?**
- Training is expensive, don't run daily
- Scoring can use cached model (alias)
- Feature updates are independent of model version
- Easier debugging and monitoring

### Model Artifacts for Metadata

**Problem:** K-Means returns `int`, how do you know what cluster 0 means?

**Solution:** Save cluster definitions as JSON artifact with the model:
```python
cluster_definitions = {
    "0": {"name": "Nit", "description": "Tight passive", "strategy": "Steal blinds"},
    "1": {"name": "LAG", "description": "Loose aggressive", "strategy": "Call lighter"}
}

# Save as artifact
with open("/tmp/cluster_definitions.json", "w") as f:
    json.dump(cluster_definitions, f)
mlflow.log_artifact("/tmp/cluster_definitions.json")

# Load in scoring notebook
artifact_path = client.download_artifacts(run_id, "cluster_definitions.json")
definitions = json.load(open(artifact_path))
```

### Pipeline Pattern (Scaler + Model)

**Don't save scaler separately** - wrap in sklearn Pipeline:
```python
from sklearn.pipeline import Pipeline

pipeline = Pipeline([
    ('scaler', StandardScaler()),
    ('model', KMeans(n_clusters=3))
])
pipeline.fit(X_train)

# Now prediction is clean - no separate scaling step
cluster = pipeline.predict(raw_features)  # Scales internally
```

### Job Configuration for ML

```yaml
tasks:
  - task_key: update_features
    notebook_task:
      notebook_path: ../src/notebooks/ml/update_features.py
    job_cluster_key: ml_cluster

  - task_key: train_model
    depends_on:
      - task_key: update_features
    notebook_task:
      notebook_path: ../src/notebooks/ml/train_model.py
    job_cluster_key: ml_cluster

  - task_key: score_batch
    depends_on:
      - task_key: train_model
    notebook_task:
      notebook_path: ../src/notebooks/ml/score_batch.py
    job_cluster_key: ml_cluster

job_clusters:
  - job_cluster_key: ml_cluster
    new_cluster:
      spark_version: "15.4.x-cpu-ml-scala2.12"
      node_type_id: "Standard_DS3_v2"
      num_workers: 0  # Single node for small workloads
      spark_conf:
        "spark.databricks.cluster.profile": "singleNode"
        "spark.master": "local[*, 4]"
```

### Model Aliases Workflow

After training, set alias for production use:
```bash
# Via CLI
databricks unity-catalog models set-alias \
  --full-name poker.ml.player_clustering \
  --alias champion \
  --version-num 3

# Or via API in notebook
client.set_registered_model_alias(
    name="poker.ml.player_clustering",
    alias="champion", 
    version="3"
)
```

### ML Tables Reference

| Table | Type | Description |
|-------|------|-------------|
| `poker.ml.player_features` | Feature Table | Player statistics for ML |
| `poker.ml.cluster_definitions` | Reference | Cluster ID → name mapping |
| `poker.gold.player_clusters` | Output | Players with assigned clusters |
| `poker.ml.player_clustering` | Model | Registered model in Unity Catalog |
