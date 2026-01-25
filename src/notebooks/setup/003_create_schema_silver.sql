-- Databricks notebook source
-- Create silver schema for processed/matched data
-- Requires: catalog poker exists

USE CATALOG poker;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Processed data layer - matched hands, player mappings';

-- COMMAND ----------

-- Create volume for silver processed files
CREATE VOLUME IF NOT EXISTS poker.silver.silver
COMMENT 'Silver volume for processed/intermediate files';

-- COMMAND ----------

-- Verify schema and volume
SHOW VOLUMES IN poker.silver;
