-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Bronze Table: analysis_result
-- MAGIC Raw JSON data from screenshot analysis

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS poker.bronze.analysis_result (
    raw_content STRING COMMENT 'Raw JSON content of the analysis file',
    file_name STRING COMMENT 'Original file name',
    file_path STRING COMMENT 'Full path to the source file',
    ingested_at TIMESTAMP COMMENT 'Timestamp when the record was ingested'
)
USING DELTA
COMMENT 'Raw analysis results from screenshot processing'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
