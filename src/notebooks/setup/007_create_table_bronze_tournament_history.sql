-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Bronze Table: tournament_history
-- MAGIC Raw text data from GGPoker tournament history files

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.bronze.tournament_history (
    raw_content STRING COMMENT 'Raw text content of the tournament history file',
    file_name STRING COMMENT 'Original file name',
    file_path STRING COMMENT 'Full path to the source file',
    ingested_at TIMESTAMP COMMENT 'Timestamp when the record was ingested'
)
USING DELTA
COMMENT 'Raw tournament history files from GGPoker'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
