# Copilot Instructions for poker_analyzer

## Databricks Asset Bundle Tips

### Naming Conventions
- Setup notebooks use 3-digit prefix: `{nnn}_{action}_{object}.sql` - this is our **migration framework**
- Examples: `001_create_catalog_poker.sql`, `002_create_schema_bronze.sql`
- Master notebook `run_all_setup.py` executes all numbered notebooks in order
- New migrations = new numbered notebook (e.g., `005_create_table_xyz.sql`)
- Never modify existing migration numbers - always add new ones

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

### Project Structure
```
poker_analyzer/
├── databricks.yml
├── resources/
│   └── *.yml          # Job definitions
└── src/
    ├── notebooks/
    │   └── setup/     # Infrastructure notebooks (SQL)
    └── package/       # Python modules for imports
```

### Variables
- Always use variables for values that might change between environments
- Define in `databricks.yml` under `variables:` with defaults
- Override per target if needed
