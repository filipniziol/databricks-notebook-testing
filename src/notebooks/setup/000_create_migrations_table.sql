-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Migration Tracking Table
-- MAGIC Tracks which setup notebooks have been executed successfully

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.bronze._migrations (
    migration_name      STRING      NOT NULL    COMMENT 'Notebook name without extension (e.g. 001_create_catalog_poker)',
    executed_at         TIMESTAMP   NOT NULL    COMMENT 'When migration was executed',
    execution_time_sec  DOUBLE                  COMMENT 'How long migration took in seconds',
    status              STRING      NOT NULL    COMMENT 'SUCCESS or FAILED'
)
USING DELTA
COMMENT 'Migration tracking - prevents re-running already executed setup notebooks'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);
