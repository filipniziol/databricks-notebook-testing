-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Bronze Table: hand_history
-- MAGIC Raw text data from GGPoker hand history files

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.bronze.hand_history (
    raw_content STRING COMMENT 'Raw text content of the hand history file',
    file_name STRING COMMENT 'Original file name',
    ingested_at TIMESTAMP COMMENT 'Timestamp when the record was ingested'
)
USING DELTA
COMMENT 'Raw hand history files from GGPoker'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
